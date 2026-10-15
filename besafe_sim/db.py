"""SQLite store for the integration worker.

Correctness never depends on worker memory: ownership is a conditional
UPDATE (lease), every outcome write is conditional on a fencing token and the
expected status, and customer messages are outbox rows with unique
idempotency keys. Anything that survives a crash is in these tables.
"""

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    id            INTEGER PRIMARY KEY,
    email         TEXT NOT NULL,
    phone         TEXT NOT NULL,
    ghl_contact_id TEXT
);

-- One row per ServiceM8 job. Identity is the ServiceM8 job UUID, never the
-- customer's email or phone (both jobs share those).
CREATE TABLE IF NOT EXISTS jobs (
    sm8_uuid        TEXT PRIMARY KEY,
    job_number      TEXT NOT NULL,
    customer_id     INTEGER NOT NULL REFERENCES customers(id),
    property        TEXT NOT NULL,
    status          TEXT NOT NULL,
    start_at        TEXT,
    sm8_edit_date   TEXT NOT NULL,          -- version guard
    content_hash    TEXT NOT NULL,          -- detects same-second edits
    ghl_opportunity_id TEXT,
    updated_at      TEXT NOT NULL
);

-- Every inbound ServiceM8 webhook delivery, including the ones refused.
-- dedupe_key = job UUID + edit_date carried by the event. Duplicates are
-- harmless by construction (the worker compares ServiceM8's current version
-- and content with the stored one); the key classifies them for the audit.
CREATE TABLE IF NOT EXISTS inbound_events (
    id          INTEGER PRIMARY KEY,
    event_id    TEXT NOT NULL,
    dedupe_key  TEXT NOT NULL,
    sm8_uuid    TEXT NOT NULL,
    event_edit_date TEXT NOT NULL,
    received_at TEXT NOT NULL,
    outcome     TEXT NOT NULL,              -- applied | stale | duplicate | no_change | unknown_job
    detail      TEXT
);
CREATE INDEX IF NOT EXISTS ix_inbound_events_key ON inbound_events (dedupe_key);

-- Customer-facing messages (confirmations and reminders). idempotency_key is
-- bound to the job version that produced the message; confirms_start_at is
-- what the message asserts and is re-checked against ServiceM8 before sending.
CREATE TABLE IF NOT EXISTS outbox (
    id               INTEGER PRIMARY KEY,
    idempotency_key  TEXT NOT NULL UNIQUE,
    sm8_uuid         TEXT NOT NULL REFERENCES jobs(sm8_uuid),
    kind             TEXT NOT NULL,
    contact_id       TEXT NOT NULL,
    body             TEXT NOT NULL,
    confirms_start_at TEXT NOT NULL,
    status           TEXT NOT NULL,         -- pending | in_flight | uncertain | sent | superseded | needs_review
    sends            INTEGER NOT NULL DEFAULT 0,
    lease_owner      TEXT,
    lease_until      TEXT,
    fencing_token    INTEGER NOT NULL DEFAULT 0,
    first_send_at    TEXT,
    resend_eligible_at TEXT,
    next_check_at    TEXT,                  -- backoff between recovery checks
    provider_message_id TEXT,
    resolution       TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_attempts (
    id            INTEGER PRIMARY KEY,
    outbox_id     INTEGER NOT NULL REFERENCES outbox(id),
    worker        TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    at            TEXT NOT NULL,
    step          TEXT NOT NULL,
    result        TEXT NOT NULL
);

-- Queued customer follow-ups (reminders).
CREATE TABLE IF NOT EXISTS followups (
    id          INTEGER PRIMARY KEY,
    sm8_uuid    TEXT NOT NULL REFERENCES jobs(sm8_uuid),
    kind        TEXT NOT NULL,
    due_at      TEXT NOT NULL,
    status      TEXT NOT NULL,              -- queued | paused | dispatched | cancelled
    reason      TEXT,
    UNIQUE (sm8_uuid, kind, due_at)
);

-- Shared Outlook mailbox ingestion. graph_id is the unique key, so the
-- webhook path and the delta-poll path can both deliver the same message.
CREATE TABLE IF NOT EXISTS mailbox_cursor (
    mailbox      TEXT PRIMARY KEY,
    delta_link   TEXT,
    synced_through TEXT
);
CREATE TABLE IF NOT EXISTS outbound_mail_index (
    internet_message_id TEXT PRIMARY KEY,
    sm8_uuid            TEXT NOT NULL REFERENCES jobs(sm8_uuid)
);
CREATE TABLE IF NOT EXISTS inbound_emails (
    graph_id       TEXT PRIMARY KEY,
    from_addr      TEXT NOT NULL,
    subject        TEXT,
    body           TEXT NOT NULL,
    in_reply_to    TEXT,
    received_at    TEXT NOT NULL,
    ingested_via   TEXT NOT NULL,           -- webhook | delta
    ingested_at    TEXT NOT NULL,
    status         TEXT NOT NULL            -- new | awaiting_review | resolved
);

CREATE TABLE IF NOT EXISTS ai_extractions (
    id             INTEGER PRIMARY KEY,
    graph_id       TEXT NOT NULL REFERENCES inbound_emails(graph_id),
    model          TEXT NOT NULL,
    raw            TEXT NOT NULL,
    confidence     REAL,
    validation     TEXT NOT NULL,           -- accepted_pending_approval | rejected
    reasons        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_tasks (
    id          INTEGER PRIMARY KEY,
    kind        TEXT NOT NULL,
    subject     TEXT NOT NULL,
    evidence    TEXT NOT NULL,
    status      TEXT NOT NULL,              -- open | resolving | resolved
    resolution  TEXT,
    resolved_by TEXT,
    created_at  TEXT NOT NULL,
    resolved_at TEXT
);
-- At most one live task per subject: a retried step cannot open a second one.
CREATE UNIQUE INDEX IF NOT EXISTS uq_live_review ON review_tasks (kind, subject)
    WHERE status IN ('open', 'resolving');

CREATE TABLE IF NOT EXISTS audit_log (
    id      INTEGER PRIMARY KEY,
    at      TEXT NOT NULL,
    actor   TEXT NOT NULL,
    action  TEXT NOT NULL,
    entity  TEXT NOT NULL,
    detail  TEXT
);
"""


def connect(path):
    # Autocommit mode; transactions are opened explicitly with BEGIN IMMEDIATE
    # so a writer takes the write lock before it reads state it will change.
    conn = sqlite3.connect(path, isolation_level=None, timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init(conn):
    conn.executescript(SCHEMA)


def audit(conn, clock, actor, action, entity, detail=""):
    conn.execute(
        "INSERT INTO audit_log (at, actor, action, entity, detail) VALUES (?,?,?,?,?)",
        (clock.now_iso(), actor, action, entity, detail),
    )
