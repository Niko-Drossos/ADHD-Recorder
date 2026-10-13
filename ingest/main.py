"""
ADHD Recall — audio ingest, transcription, and recall API.

Flow:
  phone --(POST /ingest, 5-min audio chunk)--> queued on disk + DB row
  background worker --> faster-whisper --> transcript written back to DB
  phone --(GET /summary?minutes=10)--> Ollama summarizes recent transcripts

Everything is local. Nothing leaves the homelab.
"""

import asyncio
import hashlib
import hmac
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("recall")

DATABASE_URL = os.environ["DATABASE_URL"]
INGEST_TOKEN = os.environ["INGEST_TOKEN"]
AUDIO_DIR = Path(os.environ.get("AUDIO_DIR", "/audio"))

WHISPER_MODEL = os.environ.get("WHISPER_MODEL", "small")
WHISPER_DEVICE = os.environ.get("WHISPER_DEVICE", "cpu")
WHISPER_COMPUTE_TYPE = os.environ.get("WHISPER_COMPUTE_TYPE", "int8")
WHISPER_LANGUAGE = os.environ.get("WHISPER_LANGUAGE", "en") or None

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://192.168.1.110:11434").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1:8b")

KEEP_AUDIO_DAYS = int(os.environ.get("KEEP_AUDIO_DAYS", "3"))

# Whisper is CPU-bound and the model is loaded once. Serialize work through a
# queue so concurrent uploads don't thrash memory or oversubscribe cores.
work_queue: asyncio.Queue[int] = asyncio.Queue()
_model = None
pool: AsyncConnectionPool | None = None


# --------------------------------------------------------------------------
# auth
# --------------------------------------------------------------------------

async def require_token(authorization: str = Header(default="")) -> None:
    """Shared-secret auth. Tailscale is the real perimeter; this is defense in depth."""
    expected = f"Bearer {INGEST_TOKEN}"
    # Constant-time compare to avoid leaking the token via timing.
    if not hmac.compare_digest(authorization, expected):
        raise HTTPException(status_code=401, detail="unauthorized")


# --------------------------------------------------------------------------
# transcription worker
# --------------------------------------------------------------------------

def get_model():
    global _model
    if _model is None:
        from faster_whisper import WhisperModel
        log.info("loading whisper model=%s device=%s compute=%s",
                 WHISPER_MODEL, WHISPER_DEVICE, WHISPER_COMPUTE_TYPE)
        _model = WhisperModel(
            WHISPER_MODEL,
            device=WHISPER_DEVICE,
            compute_type=WHISPER_COMPUTE_TYPE,
            download_root="/models",
        )
        log.info("whisper model loaded")
    return _model


def transcribe_sync(path: str) -> tuple[str, float, str]:
    """Blocking transcription. Runs in a thread so the event loop stays responsive."""
    model = get_model()
    segments, info = model.transcribe(
        path,
        language=WHISPER_LANGUAGE,
        # VAD filtering matters a lot here: most of a 5-minute ambient chunk is
        # silence, and without this Whisper hallucinates text into the gaps.
        vad_filter=True,
        vad_parameters={"min_silence_duration_ms": 1000},
        beam_size=5,
        condition_on_previous_text=False,
    )
    text = " ".join(seg.text.strip() for seg in segments).strip()
    return text, float(info.duration), info.language


async def worker() -> None:
    """Single consumer: pulls chunk ids, transcribes, writes results back."""
    while True:
        chunk_id = await work_queue.get()
        try:
            async with pool.connection() as conn:
                row = await (await conn.execute(
                    "SELECT audio_path FROM chunks WHERE id = %s", (chunk_id,)
                )).fetchone()
            if not row:
                continue
            audio_path = row[0]

            text, duration, lang = await asyncio.to_thread(transcribe_sync, audio_path)
            status = "done" if text else "empty"

            async with pool.connection() as conn:
                await conn.execute(
                    """UPDATE chunks
                       SET transcript = %s, duration_sec = %s, language = %s,
                           status = %s, error = NULL
                       WHERE id = %s""",
                    (text, duration, lang, status, chunk_id),
                )
            log.info("chunk %s transcribed: %s chars, %.1fs audio", chunk_id, len(text), duration)

        except Exception as exc:
            log.exception("chunk %s failed", chunk_id)
            try:
                async with pool.connection() as conn:
                    await conn.execute(
                        "UPDATE chunks SET status = 'failed', error = %s WHERE id = %s",
                        (str(exc)[:500], chunk_id),
                    )
            except Exception:
                log.exception("could not record failure for chunk %s", chunk_id)
        finally:
            work_queue.task_done()


async def requeue_pending() -> None:
    """After a restart, pick up anything that was mid-flight."""
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            "SELECT id FROM chunks WHERE status = 'pending' ORDER BY recorded_at"
        )).fetchall()
    for (chunk_id,) in rows:
        work_queue.put_nowait(chunk_id)
    if rows:
        log.info("requeued %d pending chunks", len(rows))


async def retention_loop() -> None:
    """Delete audio files older than KEEP_AUDIO_DAYS. Transcripts are kept forever."""
    while True:
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=KEEP_AUDIO_DAYS)
            async with pool.connection() as conn:
                rows = await (await conn.execute(
                    """SELECT id, audio_path FROM chunks
                       WHERE recorded_at < %s AND audio_path IS NOT NULL
                         AND status IN ('done', 'empty')""",
                    (cutoff,),
                )).fetchall()
                for chunk_id, path in rows:
                    Path(path).unlink(missing_ok=True)
                    await conn.execute(
                        "UPDATE chunks SET audio_path = NULL WHERE id = %s", (chunk_id,)
                    )
            if rows:
                log.info("retention: pruned %d audio files", len(rows))
        except Exception:
            log.exception("retention pass failed")
        await asyncio.sleep(3600)


# --------------------------------------------------------------------------
# app lifecycle
# --------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=5, open=False)
    await pool.open(wait=True)

    tasks = [
        asyncio.create_task(worker()),
        asyncio.create_task(retention_loop()),
    ]
    await requeue_pending()
    # Warm the model so the first real upload isn't slow.
    asyncio.create_task(asyncio.to_thread(get_model))

    yield

    for t in tasks:
        t.cancel()
    await pool.close()


app = FastAPI(title="ADHD Recall", lifespan=lifespan)


# --------------------------------------------------------------------------
# endpoints
# --------------------------------------------------------------------------

@app.get("/health")
async def health():
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")
    return {"ok": True, "queue_depth": work_queue.qsize(), "model_loaded": _model is not None}


@app.post("/ingest", dependencies=[Depends(require_token)])
async def ingest(
    audio: UploadFile = File(...),
    recorded_at: str = Form(...),   # ISO 8601, e.g. 2026-08-18T14:32:00Z
    device: str = Form(default="phone"),
):
    """Accept one audio chunk from the phone and queue it for transcription."""
    try:
        ts = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(status_code=400, detail="recorded_at must be ISO 8601")

    body = await audio.read()
    if not body:
        raise HTTPException(status_code=400, detail="empty upload")

    digest = hashlib.sha256(body).hexdigest()

    # Idempotency: if the phone retried an upload whose 200 it never saw,
    # return the existing row instead of transcribing the same audio twice.
    async with pool.connection() as conn:
        existing = await (await conn.execute(
            "SELECT id, status FROM chunks WHERE audio_sha256 = %s", (digest,)
        )).fetchone()
    if existing:
        return {"id": existing[0], "status": existing[1], "duplicate": True}

    suffix = Path(audio.filename or "chunk.opus").suffix or ".opus"
    dest = AUDIO_DIR / f"{ts.strftime('%Y%m%dT%H%M%SZ')}-{digest[:12]}{suffix}"
    dest.write_bytes(body)

    async with pool.connection() as conn:
        row = await (await conn.execute(
            """INSERT INTO chunks (recorded_at, audio_path, audio_sha256, device, status)
               VALUES (%s, %s, %s, %s, 'pending')
               RETURNING id""",
            (ts, str(dest), digest, device),
        )).fetchone()

    chunk_id = row[0]
    work_queue.put_nowait(chunk_id)
    log.info("accepted chunk %s (%d bytes) recorded_at=%s", chunk_id, len(body), ts)
    return {"id": chunk_id, "status": "pending", "queue_depth": work_queue.qsize()}


async def fetch_window(minutes: int) -> list[tuple[datetime, str]]:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            """SELECT recorded_at, transcript FROM chunks
               WHERE recorded_at >= %s AND status = 'done'
                 AND transcript IS NOT NULL AND transcript <> ''
               ORDER BY recorded_at""",
            (cutoff,),
        )).fetchall()
    return rows


@app.get("/transcript", dependencies=[Depends(require_token)])
async def transcript(minutes: int = 10):
    """Raw transcript for the last N minutes."""
    rows = await fetch_window(minutes)
    return {
        "minutes": minutes,
        "chunks": [{"recorded_at": r[0].isoformat(), "text": r[1]} for r in rows],
    }


@app.get("/search", dependencies=[Depends(require_token)])
async def search(q: str, limit: int = 20):
    """Full-text search across all transcripts — 'when did I mention the dentist?'"""
    async with pool.connection() as conn:
        rows = await (await conn.execute(
            """SELECT recorded_at, transcript
               FROM chunks
               WHERE to_tsvector('english', coalesce(transcript, ''))
                     @@ plainto_tsquery('english', %s)
               ORDER BY recorded_at DESC
               LIMIT %s""",
            (q, limit),
        )).fetchall()
    return {"query": q, "results": [{"recorded_at": r[0].isoformat(), "text": r[1]} for r in rows]}


SUMMARY_PROMPT = """You are helping someone with ADHD re-orient after losing their train of thought.

Below is a rough, automatically-transcribed record of what was said near them over the last {minutes} minutes. It is noisy: it may contain background TV, other people talking, half-sentences, and transcription errors. Ignore anything that looks like noise.

Answer in this format, briefly:
- **What you were doing:** one or two sentences.
- **Open loops:** anything that sounded like an unfinished task, intention, or "I should...". Bullet points. Omit this section entirely if there were none.
- **Specifics worth keeping:** names, numbers, times, or commitments that were mentioned. Omit if none.

If the transcript is too sparse or noisy to tell, say so plainly instead of guessing.

TRANSCRIPT:
{transcript}"""


@app.get("/summary", dependencies=[Depends(require_token)])
async def summary(minutes: int = 10):
    """The main event: ask the local LLM what you were just doing."""
    rows = await fetch_window(minutes)
    if not rows:
        return {"minutes": minutes, "summary": "No transcribed audio in that window yet."}

    joined = "\n".join(f"[{r[0].strftime('%H:%M')}] {r[1]}" for r in rows)
    prompt = SUMMARY_PROMPT.format(minutes=minutes, transcript=joined)

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            text = resp.json().get("response", "").strip()
    except Exception as exc:
        log.exception("ollama call failed")
        return JSONResponse(
            status_code=502,
            content={"error": f"LLM unavailable: {exc}", "raw_transcript": joined},
        )

    return {"minutes": minutes, "chunks": len(rows), "summary": text}
