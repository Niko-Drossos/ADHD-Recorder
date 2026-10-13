# ADHD Recorder

Continuous audio capture on the phone → 5-minute chunks streamed to the homelab over Tailscale → transcribed locally with faster-whisper → stored in Postgres → summarized on demand by a local LLM.

Nothing leaves the network. No cloud APIs.

```
Pixel (Automate)  ──POST /ingest──►  recall-ingest  ──►  faster-whisper
                                          │                    │
                                          ▼                    ▼
                                       Postgres  ◄──────  transcript
                                          │
Pixel (Automate)  ──GET /summary──►       │  ──►  Ollama  ──►  summary
```

## Setup

```bash
cp .env.example .env
openssl rand -hex 32          # paste into INGEST_TOKEN
tailscale ip -4               # paste into BIND_ADDR
$EDITOR .env

docker compose up -d --build
docker compose logs -f ingest # first boot downloads the whisper model
```

Verify:

```bash
curl -s http://<tailscale-ip>:8000/health
# {"ok":true,"queue_depth":0,"model_loaded":true}
```

## API

All endpoints except `/health` require `Authorization: Bearer $INGEST_TOKEN`.

| Method | Path | Purpose |
|---|---|---|
| POST | `/ingest` | Upload one audio chunk (`audio` file, `recorded_at` ISO-8601, optional `device`) |
| GET | `/summary?minutes=10` | **The main one.** LLM summary of what you were just doing |
| GET | `/transcript?minutes=10` | Raw transcript text for the window |
| GET | `/search?q=dentist` | Full-text search across all history |
| GET | `/health` | Liveness + queue depth |

Test an upload by hand:

```bash
curl -X POST http://<tailscale-ip>:8000/ingest \
  -H "Authorization: Bearer $INGEST_TOKEN" \
  -F "audio=@test.opus" \
  -F "recorded_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
```

## Automate flow 1 — recorder/uploader

Runs continuously. Start on boot.

1. **Variable Set** — `%server` = `http://<tailscale-ip>:8000`, `%token` = your token, `%queue` = `/storage/emulated/0/RecallQueue/`
2. **Loop:**
   1. **Variable Set** — `%started` = `now()` *(capture the timestamp before recording, not after)*
   2. **Record audio** — 300 seconds, output to `%queue` + `%started` + `.m4a`
   3. **Fork** → continue the loop immediately; upload happens on the other branch so recording never pauses
3. **Upload branch:**
   1. **HTTP request** — POST `%server/ingest`, multipart: `audio` = file, `recorded_at` = ISO-8601 of `%started`
   2. Header: `Authorization: Bearer %token`
   3. **If** response code == 200 → **Delete file**
   4. **Else** → leave it in `%queue`; the retry flow drains the folder when back on Wi-Fi

Set the flow's "keep device awake" / foreground-service option, and exempt Automate from battery optimization, or Android will kill it.

## Automate flow 2 — retry drainer

Every 15 min, or on Wi-Fi-connected: list `%queue`, POST each file, delete on success. This is what makes the system survive being away from home.

## Automate flow 3 — the recall button

Bind to a home-screen shortcut or quick-settings tile:

1. **HTTP request** — GET `%server/summary?minutes=10` with the auth header
2. **Speak text** (TTS) or **Notification** with the `summary` field

## Tuning

**Chunk length.** 300s is a reasonable default. Shorter chunks mean fresher data — a 5-minute chunk isn't transcribed until it's finished, so worst case your "last 10 minutes" is missing the most recent 5. If that lag bothers you, drop to 120s; the cost is more HTTP requests and more per-chunk Whisper overhead.

**Whisper model.** `small` is the sweet spot on CPU. If transcripts are mushy, try `medium`; if the queue backs up (check `/health` queue_depth), drop to `base`. With a GPU, set `WHISPER_DEVICE=cuda`, `WHISPER_COMPUTE_TYPE=float16`, and use `large-v3`.

**VAD is doing heavy lifting.** Ambient recording is mostly silence, and Whisper hallucinates confident nonsense into silence. `vad_filter=True` is what keeps the database from filling with phantom text. Don't disable it.

**Battery.** Continuous recording plus periodic upload is a real drain. Plan on charging midday, and prefer Wi-Fi-only upload.

## Things to think about

**Retention.** Audio is pruned after `KEEP_AUDIO_DAYS`; transcripts persist forever. That's a deliberate default — the transcripts are what make the feature work and they're far smaller. But it does mean accumulating a permanent, searchable record of everything said nearby. Consider a transcript retention window too:

```sql
DELETE FROM chunks WHERE recorded_at < now() - interval '90 days';
```

**Other people.** This records everyone in range, not just you. Recording-consent law varies by state and country, and some jurisdictions require all-party consent. Worth deciding deliberately where this runs — and there's a real argument for a "pause for an hour" tile, both for legal reasons and for the people around you.

**Encryption at rest.** The Postgres volume and audio directory are plaintext on the host. If the homelab disk isn't already encrypted, that's the weak link in an otherwise-private pipeline.

## Layout

```
ADHD-recorder/
├── docker-compose.yml
├── .env.example
├── .gitignore
├── db/
│   └── init.sql          # schema, applied on first DB boot
├── ingest/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py           # FastAPI app + whisper worker
└── data/                 # gitignored, created at runtime
    ├── pgdata/
    ├── audio/
    └── models/
```
