"""
ADHD Recall — audio ingest, transcription, and recall API.

Flow:
  phone --(POST /ingest, 5-min audio chunk)--> queued on disk + DB row
  background worker --> faster-whisper --> transcript written back to DB
  phone --(GET /summary?minutes=10)--> Ollama summarizes recent transcripts
  phone --(POST /pause?minutes=60)--> audio recorded in that window is discarded

Everything is local. Nothing leaves the homelab.
"""

import asyncio
import hashlib
import hmac
import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Query, UploadFile
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
_model_lock = threading.Lock()
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
    # The startup warm-up and the first upload can race here; without the lock
    # both would load the model and briefly hold two copies in memory.
    with _model_lock:
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
                    "SELECT audio_path, recorded_at FROM chunks WHERE id = %s", (chunk_id,)
                )).fetchone()
            if not row:
                continue
            audio_path, recorded_at = row

            text, duration, lang = await asyncio.to_thread(transcribe_sync, audio_path)

            # A chunk that started before a pause runs into it; we only learn its
            # length after decoding. Drop the whole thing rather than keep audio
            # from a window the user asked not to record.
            if await overlaps_pause(recorded_at, recorded_at + timedelta(seconds=duration)):
                await discard_chunk(chunk_id, audio_path)
                log.info("chunk %s overlaps a pause; discarded", chunk_id)
                continue

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


async def overlaps_pause(start: datetime, end: datetime) -> bool:
    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT 1 FROM pauses WHERE starts_at < %s AND ends_at > %s LIMIT 1",
            (end, start),
        )).fetchone()
    return row is not None


async def discard_chunk(chunk_id: int, audio_path: str | None) -> None:
    """Delete the audio and transcript but keep the row, so a retried upload
    of the same bytes is still recognised as a duplicate."""
    if audio_path:
        Path(audio_path).unlink(missing_ok=True)
    async with pool.connection() as conn:
        await conn.execute(
            """UPDATE chunks
               SET status = 'paused', transcript = NULL, audio_path = NULL, error = NULL
               WHERE id = %s""",
            (chunk_id,),
        )


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

async def ensure_schema() -> None:
    """Tables added after the first release. db/init.sql only runs when the
    Postgres volume is first created, so existing installs need these here."""
    async with pool.connection() as conn:
        await conn.execute(
            """CREATE TABLE IF NOT EXISTS pauses (
                   id         BIGSERIAL PRIMARY KEY,
                   starts_at  TIMESTAMPTZ NOT NULL,
                   ends_at    TIMESTAMPTZ NOT NULL,
                   created_at TIMESTAMPTZ NOT NULL DEFAULT now()
               )"""
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=5, open=False)
    await pool.open(wait=True)
    await ensure_schema()

    tasks = [
        asyncio.create_task(worker()),
        asyncio.create_task(retention_loop()),
        # Warm the model so the first real upload isn't slow.
        asyncio.create_task(asyncio.to_thread(get_model)),
    ]
    await requeue_pending()

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

    # Recorded while paused: say 200 so the phone deletes its copy, but store nothing.
    if await overlaps_pause(ts, ts):
        log.info("discarded chunk recorded_at=%s (paused)", ts)
        return {"id": None, "status": "paused", "discarded": True}

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
               ON CONFLICT (audio_sha256) DO NOTHING
               RETURNING id""",
            (ts, str(dest), digest, device),
        )).fetchone()
        if row is None:
            # Lost a race with a concurrent retry of the same upload.
            existing = await (await conn.execute(
                "SELECT id, status, audio_path FROM chunks WHERE audio_sha256 = %s", (digest,)
            )).fetchone()
    if row is None:
        if existing[2] != str(dest):
            dest.unlink(missing_ok=True)
        return {"id": existing[0], "status": existing[1], "duplicate": True}

    chunk_id = row[0]
    work_queue.put_nowait(chunk_id)
    log.info("accepted chunk %s (%d bytes) recorded_at=%s", chunk_id, len(body), ts)
    return {"id": chunk_id, "status": "pending", "queue_depth": work_queue.qsize()}


async def fetch_window(minutes: int) -> list[tuple[datetime, str]]:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    async with pool.connection() as conn:
        # recorded_at is when a chunk *started*. Match on when it ended, or a
        # 5-minute chunk that began 12 minutes ago would be missing from the
        # "last 10 minutes". The first condition just keeps the index usable.
        rows = await (await conn.execute(
            """SELECT recorded_at, transcript FROM chunks
               WHERE recorded_at >= %s - interval '1 hour'
                 AND recorded_at + make_interval(secs => coalesce(duration_sec, 0)) >= %s
                 AND status = 'done'
                 AND transcript IS NOT NULL AND transcript <> ''
               ORDER BY recorded_at""",
            (cutoff, cutoff),
        )).fetchall()
    return rows


@app.get("/transcript", dependencies=[Depends(require_token)])
async def transcript(minutes: int = Query(10, ge=1, le=7 * 24 * 60)):
    """Raw transcript for the last N minutes."""
    rows = await fetch_window(minutes)
    return {
        "minutes": minutes,
        "chunks": [{"recorded_at": r[0].isoformat(), "text": r[1]} for r in rows],
    }


@app.get("/search", dependencies=[Depends(require_token)])
async def search(q: str, limit: int = Query(20, ge=1, le=200)):
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
async def summary(minutes: int = Query(10, ge=1, le=24 * 60)):
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


# --------------------------------------------------------------------------
# pause
# --------------------------------------------------------------------------

async def active_pause_end() -> datetime | None:
    async with pool.connection() as conn:
        row = await (await conn.execute(
            "SELECT max(ends_at) FROM pauses WHERE starts_at <= now() AND ends_at > now()"
        )).fetchone()
    return row[0]


def pause_status(until: datetime | None) -> dict:
    return {"paused": until is not None, "until": until.isoformat() if until else None}


@app.get("/pause", dependencies=[Depends(require_token)])
async def get_pause():
    """Whether recording is currently paused. The phone checks this before each chunk."""
    return pause_status(await active_pause_end())


@app.post("/pause", dependencies=[Depends(require_token)])
async def start_pause(minutes: int = Query(60, ge=1, le=24 * 60)):
    """Stop keeping audio for the next N minutes. Calling again while paused
    sets a new end time measured from now, so the tile can be pressed again
    to extend (or shorten) the pause."""
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE pauses SET ends_at = now() WHERE starts_at <= now() AND ends_at > now()"
        )
        row = await (await conn.execute(
            """INSERT INTO pauses (starts_at, ends_at)
               VALUES (now(), now() + make_interval(mins => %s))
               RETURNING ends_at""",
            (minutes,),
        )).fetchone()
    log.info("paused until %s", row[0])
    return pause_status(row[0])


@app.delete("/pause", dependencies=[Depends(require_token)])
async def end_pause():
    """Resume recording now."""
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE pauses SET ends_at = now() WHERE starts_at <= now() AND ends_at > now()"
        )
    log.info("pause ended")
    return pause_status(None)
