"""Integration worker: ServiceM8 -> local store -> GHL, plus the shared
Outlook mailbox, follow-up dispatch, AI validation and human review.

Design rules (each one is exercised by run_demo.py and tests/):

R1  Job identity is the ServiceM8 job UUID. Email/phone identify the
    customer (one GHL contact), never the job.
R2  Inbound events are classified by (job UUID, edit_date) and then
    reconciled against ServiceM8's CURRENT record (version + content hash),
    never against the payload. An event older than the stored version is
    stale; replaying any event any number of times is a no-op; an edit in
    the same second as a previous one is still applied (content differs).
R3  Customer-facing messages (confirmations, reminders) are outbox rows
    with unique idempotency keys bound to the job version that produced
    them. State writes (GHL opportunity stage, ServiceM8 status) are
    absolute-state upserts keyed on the job UUID, made after the local
    commit, so repeating them is harmless.
R4  A worker must hold a lease to act on a row. Leases are taken with one
    conditional UPDATE ... RETURNING, so two workers cannot both win. Each
    claim increments a fencing token; every outcome write is conditional on
    that token AND the expected current status, so a worker that lost its
    lease, or whose row was superseded meanwhile, cannot overwrite state.
R5  Before ANY send (first or resend) the message is re-checked against
    ServiceM8's current record: job not cancelled, same start time.
    A row left in_flight (crash) or ended in a timeout is UNCERTAIN: the
    provider may have applied the request. Uncertain rows are never resent
    blind. Resend is permitted only when ALL hold:
      a) the worker holds a valid lease (fencing token current);
      b) a provider search STARTED at or after
         first_send_at + SEARCH_CONSISTENCY_WINDOW returns no match;
      c) the message is still true per ServiceM8 (re-read, not cached);
      d) the row has not already been resent (MAX_SENDS).
    Otherwise the row is held with backoff, closed as superseded, or sent to
    human review.
R6  Outlook ingestion is keyed on the Graph message id, so the webhook path
    and the delta-poll path are idempotent with each other. The delta poll
    is the completeness guarantee; notifications are only a latency hint.
R7  A customer's follow-ups are not dispatched (and a dispatched reminder is
    not sent) while the mailbox watermark is stale or that customer has an
    unprocessed or unreviewed reply. Any inbound reply pauses queued
    follow-ups for every job it could refer to.
R8  AI output is advisory. It is type-checked, checked against the text it
    came from, and destructive actions (cancellation) always need a human
    approval regardless of confidence.
"""

import hashlib
import json
import re
from datetime import timedelta

from . import db
from .clock import iso, parse
from .providers import ProviderTimeout, SimulatedCrash

LEASE_SECONDS = 60
SEARCH_CONSISTENCY_WINDOW = timedelta(minutes=15)
MAX_SENDS = 2  # the original send plus at most one resend
RECHECK_BACKOFF = timedelta(minutes=2)
MAILBOX_MAX_STALENESS = timedelta(minutes=5)
OPEN_REPLY_STATUSES = ("new", "awaiting_review")


class StaleLease(Exception):
    """Outcome write refused: fencing token no longer current, or the row is
    no longer in the status this worker expected."""


class ReviewError(Exception):
    """Human decision refused: task not open, or decision outside its candidates."""


def _fmt_start(start_at):
    return parse(start_at).strftime("%a %d %b %H:%M UTC")


def content_hash(job):
    return hashlib.sha256(json.dumps([job["status"], job["start_at"]]).encode()).hexdigest()[:16]


def confirmation_body(job_number, prop, start_at):
    return (f"BeSafe: your booking for job #{job_number} at {prop} "
            f"is confirmed for {_fmt_start(start_at)}. Reply to this number to change it.")


def reminder_body(job_number, prop, start_at):
    return f"BeSafe reminder: job #{job_number} at {prop} is tomorrow {_fmt_start(start_at)}."


# ---------------------------------------------------------------- import

def import_customer_jobs(conn, clock, ghl, sm8, customer, job_uuids):
    contact_id = ghl.upsert_contact(customer["email"], customer["phone"], customer["name"])
    conn.execute("INSERT INTO customers (id, email, phone, ghl_contact_id) VALUES (?,?,?,?)",
                 (customer["id"], customer["email"], customer["phone"], contact_id))
    for uuid in job_uuids:
        j = sm8.get_job(uuid)
        opp = ghl.upsert_opportunity(contact_id, uuid, f"Job #{j['job_number']} {j['property']}", j["status"])
        conn.execute(
            "INSERT INTO jobs (sm8_uuid, job_number, customer_id, property, status, start_at, sm8_edit_date,"
            " content_hash, ghl_opportunity_id, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (uuid, j["job_number"], customer["id"], j["property"], j["status"], j["start_at"], j["edit_date"],
             content_hash(j), opp, clock.now_iso()))
        _queue_reminder(conn, uuid, j["start_at"])
        db.audit(conn, clock, "importer", "job_imported", uuid, f"opp={opp} contact={contact_id}")


def _queue_reminder(conn, uuid, start_at):
    # Upsert: rescheduling X -> Y -> X must re-queue the reminder for X.
    conn.execute(
        "INSERT INTO followups (sm8_uuid, kind, due_at, status) VALUES (?,?,?,'queued') "
        "ON CONFLICT (sm8_uuid, kind, due_at) DO UPDATE SET status='queued', reason='re-queued: schedule restored' "
        "WHERE followups.status='cancelled'",
        (uuid, "reminder_24h", iso(parse(start_at) - timedelta(hours=24))))


# ---------------------------------------------------------------- R1/R2 inbound ServiceM8 events

def ingest_sm8_event(conn, clock, sm8, ghl, event):
    """event = {event_id, object_uuid, edit_date}. Returns the outcome string."""
    uuid, ev_edit = event["object_uuid"], event["edit_date"]
    dedupe_key = f"{uuid}:{ev_edit}"
    conn.execute("BEGIN IMMEDIATE")
    try:
        seen = conn.execute("SELECT 1 FROM inbound_events WHERE dedupe_key=?", (dedupe_key,)).fetchone()
        stored = conn.execute("SELECT * FROM jobs WHERE sm8_uuid=?", (uuid,)).fetchone()
        if stored is None:
            outcome, detail = "unknown_job", "job not imported; queued for review instead of failing the webhook"
            _open_review(conn, clock, "unknown_job", uuid, dict(event=event))
        elif ev_edit < stored["sm8_edit_date"]:
            outcome, detail = "stale", f"event edit_date {ev_edit} < stored {stored['sm8_edit_date']}"
        else:
            current = sm8.get_job(uuid)  # source of truth, not the payload
            if current["edit_date"] < stored["sm8_edit_date"]:
                outcome, detail = "stale", "ServiceM8 read returned an older version than stored"
            elif (current["edit_date"] == stored["sm8_edit_date"]
                  and content_hash(current) == stored["content_hash"]):
                outcome = "duplicate" if seen else "no_change"
                detail = "already applied; ServiceM8 version and content match the store"
            else:
                outcome, detail = "applied", _apply_job(conn, clock, stored, current)
        conn.execute(
            "INSERT INTO inbound_events (event_id, dedupe_key, sm8_uuid, event_edit_date, received_at, outcome,"
            " detail) VALUES (?,?,?,?,?,?,?)",
            (event["event_id"], dedupe_key, uuid, ev_edit, clock.now_iso(), outcome, detail))
        db.audit(conn, clock, "ingest", f"event_{outcome}", uuid, f"{event['event_id']}: {detail}")
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    if outcome == "applied":
        sync_opportunity(conn, ghl, uuid)
    return outcome


def sync_opportunity(conn, ghl, uuid):
    """Absolute-state upsert after commit. Safe to repeat; a failure here is
    repaired by the next event or a periodic re-sync of the same function."""
    job = conn.execute("SELECT status FROM jobs WHERE sm8_uuid=?", (uuid,)).fetchone()
    ghl.upsert_opportunity(None, uuid, None, job["status"])


def _apply_job(conn, clock, stored, current):
    uuid = current["uuid"]
    conn.execute("UPDATE jobs SET status=?, start_at=?, sm8_edit_date=?, content_hash=?, updated_at=? "
                 "WHERE sm8_uuid=?", (current["status"], current["start_at"], current["edit_date"],
                                      content_hash(current), clock.now_iso(), uuid))
    notes = [f"status {stored['status']}->{current['status']}"]

    if current["status"] == "Cancelled":
        _supersede_pending(conn, clock, uuid, "job cancelled")
        conn.execute("UPDATE followups SET status='cancelled', reason='job cancelled' "
                     "WHERE sm8_uuid=? AND status IN ('queued','paused')", (uuid,))
        notes.append("pending messages superseded, follow-ups cancelled")
    elif current["start_at"] != stored["start_at"]:
        notes.append(f"start {stored['start_at']}->{current['start_at']}")
        _supersede_pending(conn, clock, uuid, f"superseded by schedule {current['start_at']}")
        conn.execute("UPDATE followups SET status='cancelled', reason='rescheduled' "
                     "WHERE sm8_uuid=? AND status IN ('queued','paused')", (uuid,))
        _queue_reminder(conn, uuid, current["start_at"])
        contact = conn.execute("SELECT ghl_contact_id FROM customers WHERE id=?",
                               (stored["customer_id"],)).fetchone()[0]
        key = f"confirm:{uuid}:{current['start_at']}:v{current['edit_date']}"
        _enqueue(conn, clock, key, uuid, "booking_confirmation", contact,
                 confirmation_body(stored["job_number"], stored["property"], current["start_at"]),
                 current["start_at"])
        notes.append(f"outbox {key}")
    return "; ".join(notes)


def _enqueue(conn, clock, key, uuid, kind, contact, body, start_at):
    conn.execute(
        "INSERT OR IGNORE INTO outbox (idempotency_key, sm8_uuid, kind, contact_id, body, confirms_start_at, status,"
        " created_at, updated_at) VALUES (?,?,?,?,?,?,'pending',?,?)",
        (key, uuid, kind, contact, body, start_at, clock.now_iso(), clock.now_iso()))


def _supersede_pending(conn, clock, uuid, reason):
    # Rows never handed to the provider are closed. The fencing token is
    # bumped and the lease cleared, so a worker that already claimed one
    # cannot revive it. Uncertain/in-flight rows keep their state: they are
    # resolved by reconciliation, which re-checks ServiceM8 before any resend.
    conn.execute("UPDATE outbox SET status='superseded', resolution=?, fencing_token=fencing_token+1, "
                 "lease_owner=NULL, lease_until=NULL, updated_at=? WHERE sm8_uuid=? AND status='pending'",
                 (reason, clock.now_iso(), uuid))


# ---------------------------------------------------------------- R4 leases + fencing

def claim_next(conn, clock, worker):
    """Atomically claim one actionable outbox row. Returns the row or None."""
    now = clock.now_iso()
    row = conn.execute(
        """
        UPDATE outbox
           SET lease_owner = :w, lease_until = :until, fencing_token = fencing_token + 1, updated_at = :now
         WHERE id = (
                SELECT id FROM outbox
                 WHERE status IN ('pending','in_flight','uncertain')
                   AND (lease_until IS NULL OR lease_until < :now)
                   AND (next_check_at IS NULL OR next_check_at <= :now)
                 ORDER BY id LIMIT 1)
           AND (lease_until IS NULL OR lease_until < :now)
        RETURNING *
        """,
        dict(w=worker, until=iso(clock.now() + timedelta(seconds=LEASE_SECONDS)), now=now),
    ).fetchone()
    if row:
        _attempt(conn, clock, row["id"], worker, row["fencing_token"], "claim",
                 f"lease until {row['lease_until']} (status {row['status']})")
    return row


def fenced_update(conn, clock, row_id, token, worker, expect=None, **fields):
    sets = ", ".join(f"{k}=:{k}" for k in fields)
    sql = f"UPDATE outbox SET {sets}, updated_at=:now WHERE id=:id AND fencing_token=:token"
    params = dict(fields, now=clock.now_iso(), id=row_id, token=token)
    if expect:
        sql += " AND status IN (%s)" % ",".join(f":s{i}" for i in range(len(expect)))
        params.update({f"s{i}": s for i, s in enumerate(expect)})
    cur = conn.execute(sql, params)
    if cur.rowcount != 1:
        current = conn.execute("SELECT fencing_token, lease_owner, status FROM outbox WHERE id=?",
                               (row_id,)).fetchone()
        why = (f"token {token} is stale; current token {current[0]}" if current[0] != token
               else f"status is now '{current[2]}', expected {list(expect)}")
        _attempt(conn, clock, row_id, worker, token, "write_rejected", why)
        raise StaleLease(f"{worker} write refused: {why}")


def _attempt(conn, clock, row_id, worker, token, step, result):
    conn.execute("INSERT INTO outbox_attempts (outbox_id, worker, fencing_token, at, step, result) "
                 "VALUES (?,?,?,?,?,?)", (row_id, worker, token, clock.now_iso(), step, result))


# ---------------------------------------------------------------- R5 delivery

def process(conn, clock, sm8, ghl, worker, row, crash=None):
    """Process a claimed row.

    crash: None | 'before_send' | 'after_send' | 'after_timeout' injects a
    process death at that exact point (a SimulatedCrash propagates and the
    worker's in-memory state is lost; only committed DB state survives).
    """
    token = row["fencing_token"]
    if row["status"] == "pending":
        return _send(conn, clock, sm8, ghl, worker, row, token, crash)
    return _recover(conn, clock, sm8, ghl, worker, row, token, crash)


def _still_true(conn, sm8, row):
    """('ok'|'superseded'|'hold', reason). ServiceM8 is re-read, not cached."""
    current = sm8.get_job(row["sm8_uuid"])
    if current["status"] == "Cancelled":
        return "superseded", "job is Cancelled in ServiceM8"
    if current["start_at"] != row["confirms_start_at"]:
        return "superseded", f"ServiceM8 start is now {current['start_at']}, message says {row['confirms_start_at']}"
    if row["kind"] == "reminder" and customer_has_open_reply(conn, row["sm8_uuid"]):
        return "hold", "customer has an unprocessed or unreviewed reply"
    return "ok", "ServiceM8 confirms job and start time"


def _send(conn, clock, sm8, ghl, worker, row, token, crash):
    verdict, why = _still_true(conn, sm8, row)
    if verdict == "superseded":
        fenced_update(conn, clock, row["id"], token, worker, expect=(row["status"],), status="superseded",
                      resolution=f"not sent: {why}", lease_until=None, lease_owner=None)
        _attempt(conn, clock, row["id"], worker, token, "superseded", why)
        return "superseded"
    if verdict == "hold":
        fenced_update(conn, clock, row["id"], token, worker, expect=(row["status"],), lease_until=None,
                      lease_owner=None, next_check_at=iso(clock.now() + RECHECK_BACKOFF))
        _attempt(conn, clock, row["id"], worker, token, "hold", why)
        return "held"

    sends = row["sends"] + 1
    first = row["first_send_at"] or clock.now_iso()
    # Intent is committed BEFORE the external call: a crash from here on
    # leaves an in_flight row that recovery treats as uncertain.
    fenced_update(conn, clock, row["id"], token, worker, expect=("pending", "uncertain"), status="in_flight",
                  sends=sends, first_send_at=first,
                  resend_eligible_at=iso((parse(first) if sends == 1 else clock.now()) + SEARCH_CONSISTENCY_WINDOW))
    _attempt(conn, clock, row["id"], worker, token, "send_start", f"send #{sends} -> GHL ({why})")
    if crash == "before_send":
        _attempt(conn, clock, row["id"], worker, token, "crash", "process died before the GHL request")
        raise SimulatedCrash(f"{worker} died before GHL request")
    try:
        resp = ghl.send_message(row["contact_id"], row["body"])
    except ProviderTimeout as exc:
        if crash == "after_timeout":
            _attempt(conn, clock, row["id"], worker, token, "crash", f"process died handling timeout: {exc}")
            raise SimulatedCrash(f"{worker} died after GHL timeout, outcome not saved") from exc
        fenced_update(conn, clock, row["id"], token, worker, expect=("in_flight",), status="uncertain",
                      lease_until=None, lease_owner=None)
        _attempt(conn, clock, row["id"], worker, token, "send_result", f"TIMEOUT -> uncertain: {exc}")
        return "uncertain"
    if crash == "after_send":
        _attempt(conn, clock, row["id"], worker, token, "crash", "process died after GHL accepted, before save")
        raise SimulatedCrash(f"{worker} died after GHL accepted the request")
    fenced_update(conn, clock, row["id"], token, worker, expect=("in_flight",), status="sent",
                  provider_message_id=resp["messageId"], lease_until=None, lease_owner=None,
                  resolution=f"delivered: GHL response received on send #{sends}")
    _attempt(conn, clock, row["id"], worker, token, "send_result", f"OK {resp['messageId']}")
    return "sent"


def _recover(conn, clock, sm8, ghl, worker, row, token, crash):
    if row["status"] == "in_flight":
        fenced_update(conn, clock, row["id"], token, worker, expect=("in_flight",), status="uncertain")
        _attempt(conn, clock, row["id"], worker, token, "recover",
                 "found in_flight with expired lease: previous worker died; outcome unknown -> uncertain")

    searched_at = clock.now_iso()  # taken BEFORE the search: R5(b) is about when the search started
    hits = ghl.search_messages(row["contact_id"], row["body"])
    if hits:
        fenced_update(conn, clock, row["id"], token, worker, expect=("uncertain",), status="sent",
                      provider_message_id=hits[0]["id"], lease_until=None, lease_owner=None,
                      resolution=f"reconciled from provider search ({len(hits)} match)")
        _attempt(conn, clock, row["id"], worker, token, "reconcile",
                 f"search FOUND {[h['id'] for h in hits]} -> sent, no resend")
        return "reconciled"

    eligible_at = row["resend_eligible_at"]
    if searched_at < eligible_at:
        next_check = iso(clock.now() + RECHECK_BACKOFF)
        fenced_update(conn, clock, row["id"], token, worker, expect=("uncertain",), lease_until=None,
                      lease_owner=None, next_check_at=next_check)
        _attempt(conn, clock, row["id"], worker, token, "hold",
                 f"search NOT FOUND but provider search may lag; resend forbidden until {eligible_at}; "
                 f"next check {next_check}")
        return "held"

    verdict, why = _still_true(conn, sm8, row)
    if verdict == "superseded":
        fenced_update(conn, clock, row["id"], token, worker, expect=("uncertain",), status="superseded",
                      resolution=f"not resent: {why} (first send may have been delivered)",
                      lease_until=None, lease_owner=None)
        _attempt(conn, clock, row["id"], worker, token, "superseded", why)
        return "superseded"

    if row["sends"] >= MAX_SENDS:
        fenced_update(conn, clock, row["id"], token, worker, expect=("uncertain",), status="needs_review",
                      resolution="resend budget exhausted", lease_until=None, lease_owner=None)
        _open_review(conn, clock, "delivery_unverifiable", row["idempotency_key"], dict(
            outbox_id=row["id"], sends=row["sends"], first_send_at=row["first_send_at"],
            searched_at=searched_at, search_result="no match",
            reviewer_check="Open the GHL conversation for the contact and the carrier delivery report; "
                           "if no message exists, send manually and mark resolved."))
        return "needs_review"

    _attempt(conn, clock, row["id"], worker, token, "resend_permitted",
             f"search started {searched_at} >= window end {eligible_at} found nothing; lease valid; "
             f"{why}; sends={row['sends']}<{MAX_SENDS}")
    fresh = conn.execute("SELECT * FROM outbox WHERE id=?", (row["id"],)).fetchone()
    return _send(conn, clock, sm8, ghl, worker, fresh, token, crash)


def reconcile_duplicates(conn, clock, ghl):
    """Post-hoc sweep: any idempotency key with more than one provider
    message is a delivered duplicate. It cannot be undone, only reported."""
    found = []
    for row in conn.execute("SELECT * FROM outbox WHERE status='sent'").fetchall():
        hits = ghl.search_messages(row["contact_id"], row["body"])
        if len(hits) > 1:
            _open_review(conn, clock, "duplicate_delivered", row["idempotency_key"], dict(
                outbox_id=row["id"], provider_message_ids=[h["id"] for h in hits],
                created_at=[h["created_at"] for h in hits],
                attempts=[dict(r) for r in conn.execute(
                    "SELECT worker, fencing_token, at, step, result FROM outbox_attempts WHERE outbox_id=?",
                    (row["id"],))]))
            found.append(row["idempotency_key"])
    return found


# ---------------------------------------------------------------- R6 Outlook

def ingest_mail(conn, clock, outlook, graph_id, via):
    m = outlook.get_message(graph_id)
    cur = conn.execute(
        "INSERT OR IGNORE INTO inbound_emails (graph_id, from_addr, subject, body, in_reply_to, received_at,"
        " ingested_via, ingested_at, status) VALUES (?,?,?,?,?,?,?,?,'new')",
        (graph_id, m["from_addr"], m["subject"], m["body"], m["in_reply_to"], m["received_at"], via,
         clock.now_iso()))
    db.audit(conn, clock, f"mail_{via}", "ingested" if cur.rowcount else "already_ingested", graph_id)
    return cur.rowcount == 1


def sync_mailbox(conn, clock, outlook, mailbox="bookings@besafe.example"):
    row = conn.execute("SELECT delta_link FROM mailbox_cursor WHERE mailbox=?", (mailbox,)).fetchone()
    msgs, new_link = outlook.delta(row["delta_link"] if row else None)
    recovered = [m["graph_id"] for m in msgs if ingest_mail(conn, clock, outlook, m["graph_id"], "delta")]
    conn.execute("INSERT INTO mailbox_cursor (mailbox, delta_link, synced_through) VALUES (?,?,?) "
                 "ON CONFLICT(mailbox) DO UPDATE SET delta_link=excluded.delta_link, "
                 "synced_through=excluded.synced_through", (mailbox, new_link, clock.now_iso()))
    return recovered


def customer_has_open_reply(conn, uuid, exclude_graph_id=None):
    return conn.execute(
        "SELECT 1 FROM inbound_emails e JOIN customers c ON c.email=e.from_addr JOIN jobs j ON j.customer_id=c.id "
        "WHERE j.sm8_uuid=? AND e.status IN (?,?) AND e.graph_id IS NOT ?",
        (uuid, *OPEN_REPLY_STATUSES, exclude_graph_id)).fetchone() is not None


# ---------------------------------------------------------------- R7 follow-ups

def dispatch_followups(conn, clock, mailbox="bookings@besafe.example"):
    """Hand due reminders to the outbox (the outbox worker delivers them)."""
    now = clock.now()
    due = conn.execute("SELECT * FROM followups WHERE status='queued' AND due_at<=?", (iso(now),)).fetchall()
    cur = conn.execute("SELECT synced_through FROM mailbox_cursor WHERE mailbox=?", (mailbox,)).fetchone()
    stale = cur is None or now - parse(cur["synced_through"]) > MAILBOX_MAX_STALENESS
    results = []
    for f in due:
        if stale:
            db.audit(conn, clock, "followups", "held_mailbox_stale", f["sm8_uuid"],
                     f"followup {f['id']} due {f['due_at']}; mailbox synced_through "
                     f"{cur['synced_through'] if cur else None}")
            results.append((f["id"], "held: mailbox stale"))
            continue
        if customer_has_open_reply(conn, f["sm8_uuid"]):
            db.audit(conn, clock, "followups", "held_open_reply", f["sm8_uuid"], f"followup {f['id']}")
            results.append((f["id"], "held: unprocessed reply"))
            continue
        job = conn.execute("SELECT j.*, c.ghl_contact_id FROM jobs j JOIN customers c ON c.id=j.customer_id "
                           "WHERE sm8_uuid=?", (f["sm8_uuid"],)).fetchone()
        conn.execute("BEGIN IMMEDIATE")
        _enqueue(conn, clock, f"reminder:{f['sm8_uuid']}:{f['due_at']}:f{f['id']}", f["sm8_uuid"], "reminder",
                 job["ghl_contact_id"], reminder_body(job["job_number"], job["property"], job["start_at"]),
                 job["start_at"])
        conn.execute("UPDATE followups SET status='dispatched' WHERE id=? AND status='queued'", (f["id"],))
        conn.execute("COMMIT")
        results.append((f["id"], "dispatched to outbox"))
    return results


# ---------------------------------------------------------------- R8 inbound reply -> AI -> review

def process_inbound_emails(conn, clock, ai):
    out = []
    for e in conn.execute("SELECT * FROM inbound_emails WHERE status='new' ORDER BY received_at").fetchall():
        cust = conn.execute("SELECT * FROM customers WHERE email=?", (e["from_addr"],)).fetchone()
        candidates = [] if cust is None else [dict(r) for r in conn.execute(
            "SELECT sm8_uuid, job_number, property, status, start_at FROM jobs "
            "WHERE customer_id=? AND status!='Cancelled' ORDER BY job_number", (cust["id"],))]

        # Slow, fallible work (the AI call) happens outside the transaction.
        ai_raw, verdict, reasons = None, "rejected", ["unknown sender"]
        if cust is not None:
            try:
                ai_raw = ai.extract(e["body"], candidates)
                verdict, reasons = validate_extraction(e["body"], ai_raw, candidates)
            except Exception as exc:  # model/API failure is a rejection, never a crash
                verdict, reasons = "rejected", [f"ai_error: {exc!r}"]

        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT status FROM inbound_emails WHERE graph_id=?",
                        (e["graph_id"],)).fetchone()["status"] != "new":
            conn.execute("ROLLBACK")  # another processor got here first
            continue
        if cust is None:
            _open_review(conn, clock, "unknown_sender", e["graph_id"], dict(from_addr=e["from_addr"]))
        else:
            # Any reply pauses queued follow-ups for every job it could be about.
            for c in candidates:
                conn.execute("UPDATE followups SET status='paused', reason=? WHERE sm8_uuid=? AND status='queued'",
                             (f"customer reply {e['graph_id']} awaiting review", c["sm8_uuid"]))
            thread_job = None
            if e["in_reply_to"]:
                hit = conn.execute("SELECT sm8_uuid FROM outbound_mail_index WHERE internet_message_id=?",
                                   (e["in_reply_to"],)).fetchone()
                thread_job = hit["sm8_uuid"] if hit else None
            conf = ai_raw.get("confidence") if isinstance(ai_raw, dict) else None
            conn.execute("INSERT INTO ai_extractions (graph_id, model, raw, confidence, validation, reasons) "
                         "VALUES (?,?,?,?,?,?)", (e["graph_id"], ai.model, json.dumps(ai_raw, default=str),
                                                  conf if isinstance(conf, (int, float)) else None,
                                                  verdict, json.dumps(reasons)))
            suggestion = None
            if (thread_job and len(candidates) == 2 and _mentions_keep_other(e["body"])
                    and any(c["sm8_uuid"] == thread_job for c in candidates)):
                other = next(c for c in candidates if c["sm8_uuid"] != thread_job)
                suggestion = dict(cancel=[thread_job], keep=[other["sm8_uuid"]],
                                  basis=f"In-Reply-To {e['in_reply_to']} is the booking email for job "
                                        f"{_num(candidates, thread_job)}; 'keep the other' -> job "
                                        f"{other['job_number']}")
            _open_review(conn, clock, "customer_instruction", e["graph_id"], dict(
                email=dict(from_addr=e["from_addr"], subject=e["subject"], body=e["body"],
                           in_reply_to=e["in_reply_to"], ingested_via=e["ingested_via"]),
                candidates=candidates, thread_match=thread_job,
                ai=dict(raw=ai_raw, validation=verdict, reasons=reasons),
                suggestion=suggestion or "AMBIGUOUS: no thread reference; ask the customer which property",
                policy="Cancellation is destructive: human approval required regardless of AI confidence."))
            out.append(dict(graph_id=e["graph_id"], verdict=verdict, reasons=reasons, suggestion=suggestion))
        conn.execute("UPDATE inbound_emails SET status='awaiting_review' WHERE graph_id=?", (e["graph_id"],))
        conn.execute("COMMIT")
    return out


def _num(cands, uuid):
    return next(c["job_number"] for c in cands if c["sm8_uuid"] == uuid)


def _mentions_keep_other(text):
    return bool(re.search(r"\bkeep\b.*\bother\b", text, re.I))


def validate_extraction(text, ai, candidates):
    """Returns (verdict, reasons). Any malformed output is a rejection."""
    if not isinstance(ai, dict):
        return "rejected", ["schema: output is not an object"]
    actions, confidence = ai.get("actions"), ai.get("confidence")
    reasons = []
    if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
        reasons.append("schema: confidence missing or outside 0..1")
    if not isinstance(actions, list) or not actions or not all(isinstance(a, dict) for a in actions):
        return "rejected", reasons + ["schema: actions must be a non-empty list of objects"]
    if any(a.get("action") not in ("cancel", "keep", "reschedule") for a in actions):
        reasons.append("schema: unknown action")
    numbers = {c["job_number"] for c in candidates}
    if any(a.get("job_number") not in numbers for a in actions):
        reasons.append("reference: job not among this customer's open jobs")
    cancels = [a for a in actions if a.get("action") == "cancel"]
    if _mentions_keep_other(text) and len(cancels) >= len(candidates):
        reasons.append("contradiction: text says 'keep the other' but extraction cancels every job")
    if re.search(r"\bthat one\b", text, re.I) and len(cancels) != 1:
        reasons.append(f"count: 'that one' implies exactly 1 cancellation, extraction has {len(cancels)}")
    if cancels:
        reasons.append("policy: cancellation is destructive -> human approval required")
    hard = [r for r in reasons if not r.startswith("policy")]
    return ("rejected" if hard else "accepted_pending_approval"), reasons


def _open_review(conn, clock, kind, subject, evidence):
    # The partial unique index allows one live task per (kind, subject).
    conn.execute("INSERT OR IGNORE INTO review_tasks (kind, subject, evidence, status, created_at) "
                 "VALUES (?,?,?,'open',?)", (kind, subject, json.dumps(evidence, indent=1, default=str),
                                             clock.now_iso()))


def resolve_instruction(conn, clock, sm8, task_id, reviewer, cancel, keep):
    """Human decision on a customer_instruction task.

    1. Claim the task open -> resolving (conditional; a second click fails).
    2. Validate the decision against the task's own candidate jobs.
    3. Write ServiceM8 (absolute state, safe to repeat). The local store
       learns about it through the normal webhook path.
    4. Mark resolved; resume only follow-ups paused by THIS reply, and only
       if the customer has no other reply still open.
    A crash between 1 and 4 leaves the task 'resolving' (visible, re-runnable).
    """
    task = conn.execute("SELECT * FROM review_tasks WHERE id=?", (task_id,)).fetchone()
    if task is None or task["kind"] != "customer_instruction":
        raise ReviewError(f"task {task_id} is not a customer_instruction task")
    allowed = {c["sm8_uuid"] for c in json.loads(task["evidence"])["candidates"]}
    if not set(cancel) | set(keep) <= allowed or set(cancel) & set(keep):
        raise ReviewError("decision references jobs outside this task's candidates, or both cancels and keeps one")
    cur = conn.execute("UPDATE review_tasks SET status='resolving', resolved_by=? WHERE id=? AND status='open'",
                       (reviewer, task_id))
    if cur.rowcount != 1:
        raise ReviewError(f"task {task_id} is not open (status {task['status']})")

    for uuid in cancel:
        sm8.update_job(uuid, actor=f"integration (approved by {reviewer})", status="Cancelled")

    conn.execute("BEGIN IMMEDIATE")
    conn.execute("UPDATE review_tasks SET status='resolved', resolution=?, resolved_at=? WHERE id=?",
                 (json.dumps(dict(cancel=cancel, keep=keep)), clock.now_iso(), task_id))
    conn.execute("UPDATE inbound_emails SET status='resolved' WHERE graph_id=?", (task["subject"],))
    for uuid in keep:
        if not customer_has_open_reply(conn, uuid):
            conn.execute("UPDATE followups SET status='queued', reason='resumed after review' "
                         "WHERE sm8_uuid=? AND status='paused' AND reason LIKE ?",
                         (uuid, f"%{task['subject']}%"))
    db.audit(conn, clock, reviewer, "review_resolved", task["subject"], f"cancel={cancel} keep={keep}")
    conn.execute("COMMIT")
