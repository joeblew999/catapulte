-- CF-native schema for catapulte on D1. Unlike the sqlite adapter (BLOB ids),
-- ids and JSON payloads are TEXT — D1 round-trips text/integer cleanly, and a
-- fresh schema has no blob-compatibility constraint.
CREATE TABLE IF NOT EXISTS emails (
    id              TEXT PRIMARY KEY NOT NULL,
    idempotency_key TEXT,
    correlation_id  TEXT,
    subject         TEXT,
    sender          TEXT NOT NULL,
    recipients      TEXT NOT NULL,
    body            TEXT NOT NULL,
    variables       TEXT NOT NULL,
    created_at_ms   INTEGER NOT NULL DEFAULT (CAST(unixepoch('now', 'subsec') * 1000 AS INTEGER))
);

CREATE UNIQUE INDEX IF NOT EXISTS emails_idempotency_key
    ON emails(idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS emails_created_at_ms ON emails(created_at_ms);
