-- Transcript chunks: one row per uploaded audio chunk.
CREATE TABLE IF NOT EXISTS chunks (
    id              BIGSERIAL PRIMARY KEY,

    -- When the audio was actually captured on the phone (authoritative for queries).
    recorded_at     TIMESTAMPTZ NOT NULL,
    -- When the server received it. Diverges from recorded_at if the phone
    -- queued uploads while offline.
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    duration_sec    REAL,
    transcript      TEXT,

    -- pending -> done | failed | empty
    status          TEXT NOT NULL DEFAULT 'pending',
    error           TEXT,

    audio_path      TEXT,
    -- SHA256 of the uploaded bytes; prevents duplicate rows when the phone
    -- retries an upload whose response it never saw.
    audio_sha256    TEXT UNIQUE,

    device          TEXT,
    language        TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Primary access pattern: "what happened in the last N minutes"
CREATE INDEX IF NOT EXISTS chunks_recorded_at_idx
    ON chunks (recorded_at DESC);

-- For the background worker picking up unprocessed uploads after a restart.
CREATE INDEX IF NOT EXISTS chunks_status_idx
    ON chunks (status)
    WHERE status = 'pending';

-- Optional: full-text search over transcripts, for "when did I mention X?"
CREATE INDEX IF NOT EXISTS chunks_transcript_fts_idx
    ON chunks
    USING GIN (to_tsvector('english', coalesce(transcript, '')));
