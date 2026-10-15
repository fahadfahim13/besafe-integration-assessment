"""The BeSafe assessment scenario and its variants, as deterministic scripts.

Every step returns observable results; nothing here asserts. run_demo.py
prints and evaluates them, tests/ asserts on them.
"""

import json
import threading
from datetime import timedelta

from . import db, engine
from .clock import SimClock, iso
from .providers import AIExtractor, GHL, Outlook, ServiceM8, SimulatedCrash, snapshot_ghl

JOB_A = "a1f0c3e2-1041-4b6e-9c11-0000000a1041"
JOB_B = "b2e1d4f3-1042-4c7f-8d22-0000000b1042"
CUSTOMER = dict(id=1, name="Jordan Lee (synthetic)", email="jordan.lee@example.com", phone="+61400000111")
MAILBOX = "bookings@besafe.example"


class World:
    """One isolated system: store + simulated providers + clock."""

    def __init__(self, db_path, search_lag_minutes=5, log=print):
        self.db_path = db_path
        self.clock = SimClock()
        self.conn = db.connect(db_path)
        db.init(self.conn)
        self.sm8 = ServiceM8(self.clock)
        self.ghl = GHL(self.clock, search_lag_minutes=search_lag_minutes)
        self.outlook = Outlook(self.clock)
        self.ai = AIExtractor()
        self.log = log
        self.outlook.webhook = lambda gid: engine.ingest_mail(self.conn, self.clock, self.outlook, gid, "webhook")

    def t(self):
        return self.clock.now_iso()

    def at(self, minutes):
        """Move the clock to T0 + minutes."""
        target = SimClock().now() + timedelta(minutes=minutes)
        self.clock.advance(seconds=(target - self.clock.now()).total_seconds())

    def worker_conn(self):
        return db.connect(self.db_path)

    # ---- seeding
    def seed(self):
        self.sm8.seed_job(JOB_A, job_number="1041", property="12 Harbour St, Manly NSW", status="Scheduled",
                          start_at="2026-10-21T00:45:00Z", company_uuid="cust-1")
        self.sm8.seed_job(JOB_B, job_number="1042", property="7 Ridge Rd, Dee Why NSW", status="Scheduled",
                          start_at="2026-10-22T03:00:00Z", company_uuid="cust-1")
        engine.import_customer_jobs(self.conn, self.clock, self.ghl, self.sm8, CUSTOMER, [JOB_A, JOB_B])
        for num, uuid in (("1041", JOB_A), ("1042", JOB_B)):
            mid = f"<booking-{num}@besafe.example>"
            self.outlook.record_outbound(mid, f"Your BeSafe booking - job #{num}")
            self.conn.execute("INSERT INTO outbound_mail_index (internet_message_id, sm8_uuid) VALUES (?,?)",
                              (mid, uuid))
        engine.sync_mailbox(self.conn, self.clock, self.outlook, MAILBOX)

    # ---- snapshot of every system of record
    def snapshot(self):
        q = lambda sql: [dict(r) for r in self.conn.execute(sql).fetchall()]
        return dict(
            at=self.t(),
            servicem8_jobs=[dict(uuid=j["uuid"], job_number=j["job_number"], status=j["status"],
                                 start_at=j["start_at"], edit_date=j["edit_date"]) for j in self.sm8.jobs.values()],
            store_jobs=q("SELECT job_number, status, start_at, sm8_edit_date FROM jobs ORDER BY job_number"),
            inbound_events=q("SELECT event_id, event_edit_date, received_at, outcome, detail FROM inbound_events"
                             " ORDER BY id"),
            outbox=q("SELECT id, kind, idempotency_key, status, sends, fencing_token, lease_owner, lease_until,"
                     " first_send_at, resend_eligible_at, provider_message_id, resolution FROM outbox ORDER BY id"),
            followups=q("SELECT f.id, j.job_number, f.kind, f.due_at, f.status, f.reason FROM followups f "
                        "JOIN jobs j ON j.sm8_uuid=f.sm8_uuid ORDER BY f.id"),
            mailbox_cursor=q("SELECT * FROM mailbox_cursor"),
            inbound_emails=q("SELECT graph_id, from_addr, in_reply_to, ingested_via, status FROM inbound_emails"),
            ai_extractions=q("SELECT graph_id, confidence, validation, reasons FROM ai_extractions"),
            review_tasks=q("SELECT id, kind, subject, status, resolution, resolved_by FROM review_tasks ORDER BY id"),
            ghl=snapshot_ghl(self.ghl),
            outlook_dropped_notifications=list(self.outlook.dropped_notifications),
        )


def race_claim(world, workers):
    """Start N workers on separate DB connections at the same instant;
    each tries to claim the next outbox row. Returns {worker: token|None}."""
    barrier = threading.Barrier(len(workers))
    results = {}

    def run(name):
        conn = world.worker_conn()
        barrier.wait()
        row = engine.claim_next(conn, world.clock, name)
        results[name] = dict(row) if row else None
        conn.close()

    threads = [threading.Thread(target=run, args=(w,)) for w in workers]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    return results


def crash_run(world, worker, crash):
    conn = world.worker_conn()
    row = engine.claim_next(conn, world.clock, worker)
    try:
        engine.process(conn, world.clock, world.sm8, world.ghl, worker, row, crash=crash)
    except SimulatedCrash as exc:
        return f"CRASHED: {exc}"  # in-memory state gone; only committed rows survive
    finally:
        conn.close()
    return "no crash"


def recover_once(world, worker):
    conn = world.worker_conn()
    row = engine.claim_next(conn, world.clock, worker)
    if row is None:
        conn.close()
        return None, None
    try:
        res = engine.process(conn, world.clock, world.sm8, world.ghl, worker, row)
    finally:
        conn.close()
    return res, row["fencing_token"]


# ===================================================================== main scenario

def run_main(db_path, log=print):
    w = World(db_path, search_lag_minutes=5, log=log)
    ev = dict(steps=[], snapshots={})

    def step(name, result):
        ev["steps"].append(dict(at=w.t(), step=name, result=result))
        log(f"  [{w.t()}] {name}: {result}")

    w.seed()
    ev["snapshots"]["1_before"] = w.snapshot()

    # ---- 1. reschedule, late older update, duplicate
    w.at(1)
    older = w.sm8.update_job(JOB_B, actor="office", access_notes="Gate code 4471")
    w.at(2)
    newer = w.sm8.update_job(JOB_B, actor="office", start_at="2026-10-21T00:40:00Z")
    w.at(3)
    step("evt-B-201 reschedule (edit_date %s)" % newer["edit_date"],
         engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                                 dict(event_id="evt-B-201", object_uuid=JOB_B, edit_date=newer["edit_date"])))
    w.at(4)
    step("evt-B-200 older update arriving late (edit_date %s)" % older["edit_date"],
         engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                                 dict(event_id="evt-B-200", object_uuid=JOB_B, edit_date=older["edit_date"])))
    w.at(5)
    step("evt-B-201 duplicate delivery",
         engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                                 dict(event_id="evt-B-201-redelivery", object_uuid=JOB_B,
                                      edit_date=newer["edit_date"])))

    # ---- 2. GHL accepts, response times out, worker dies before saving
    w.at(6)
    w.ghl.fail_next_send = "timeout_after_accept"
    step("W1 sends confirmation; GHL accepts but times out; W1 crashes", crash_run(w, "W1", "after_timeout"))
    ev["snapshots"]["2_after_failures_delivery"] = w.snapshot()

    # ---- 3. two workers race for the unfinished item; search lags
    w.at(8)
    race = race_claim(w, ["W2", "W3"])
    winner = next(k for k, v in race.items() if v)
    step("W2 and W3 race for the expired lease",
         {k: (f"WON token={v['fencing_token']}" if v else "lost (lease held)") for k, v in race.items()})
    conn = w.worker_conn()
    row = conn.execute("SELECT * FROM outbox WHERE lease_owner=?", (winner,)).fetchone()
    step(f"{winner} recovers: provider search", engine.process(conn, w.clock, w.sm8, w.ghl, winner, row))
    conn.close()
    stale_token = race[winner]["fencing_token"]

    w.at(9)
    res, tok = recover_once(w, "W3")
    step("W3 polls at 00:09 (inside backoff)", "nothing claimable" if res is None else f"{res} token={tok}")
    w.at(10)
    res, tok = recover_once(w, "W3" if winner == "W2" else "W2")
    step("second recovery pass at 00:10 (search still inside lag)", f"{res} token={tok}")
    conn = w.worker_conn()
    try:
        engine.fenced_update(conn, w.clock, row["id"], stale_token, winner, status="sent",
                             resolution="late write from paused worker")
        step(f"{winner} (paused since 00:08) wakes late and writes with token {stale_token}", "ACCEPTED (BUG)")
    except engine.StaleLease as exc:
        step(f"{winner} (paused since 00:08) wakes late and writes with token {stale_token}", f"REJECTED: {exc}")
    conn.close()

    # ---- 4. Outlook outage: notification missed, follow-ups due
    w.at(20)
    engine.sync_mailbox(w.conn, w.clock, w.outlook, MAILBOX)
    step("mailbox delta sync before outage", "watermark 00:20")
    w.outlook.notifications_up = False
    step("OUTAGE starts", "Graph notifications and poller down")

    w.at(22)
    res, tok = recover_once(w, "W2")
    step("recovery after search catches up", f"{res} token={tok}")

    w.at(25)
    gid = w.outlook.receive(CUSTOMER["email"], "Re: Your BeSafe booking - job #1042",
                            "Cancel that one, keep the other. Thanks, Jordan",
                            in_reply_to="<booking-1042@besafe.example>")
    step("customer reply lands in shared mailbox", f"{gid}; notification DROPPED")

    w.at(41)
    step("follow-up dispatcher tick (reminder #1042 due 00:40)",
         engine.dispatch_followups(w.conn, w.clock, MAILBOX))
    w.at(46)
    step("follow-up dispatcher tick (reminder #1041 due 00:45)",
         engine.dispatch_followups(w.conn, w.clock, MAILBOX))
    ev["snapshots"]["3_after_failures_all"] = w.snapshot()

    # ---- recovery
    w.at(55)
    w.outlook.notifications_up = True
    step("OUTAGE ends; delta poll", f"recovered {engine.sync_mailbox(w.conn, w.clock, w.outlook, MAILBOX)}")
    step("follow-up dispatcher tick (mailbox fresh, reply not yet processed)",
         engine.dispatch_followups(w.conn, w.clock, MAILBOX))
    w.at(56)
    step("process inbound reply (AI + validation)", engine.process_inbound_emails(w.conn, w.clock, w.ai))
    step("follow-up dispatcher tick (after reply processed)",
         engine.dispatch_followups(w.conn, w.clock, MAILBOX) or "nothing dispatched")
    step("duplicate-delivery sweep", engine.reconcile_duplicates(w.conn, w.clock, w.ghl) or "none")
    ev["snapshots"]["4_after_automated_recovery"] = w.snapshot()

    # ---- human review
    w.at(60)
    task = w.conn.execute("SELECT id FROM review_tasks WHERE kind='customer_instruction' AND status='open'"
                          ).fetchone()
    engine.resolve_instruction(w.conn, w.clock, w.sm8, task["id"], "ops.reviewer@besafe", cancel=[JOB_B],
                               keep=[JOB_A])
    step("reviewer approves: cancel #1042, keep #1041", "ServiceM8 write issued")
    w.at(61)
    step("evt-B-202 cancellation webhook",
         engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                                 dict(event_id="evt-B-202", object_uuid=JOB_B,
                                      edit_date=w.sm8.jobs[JOB_B]["edit_date"])))
    w.at(62)
    engine.sync_mailbox(w.conn, w.clock, w.outlook, MAILBOX)
    step("follow-up dispatcher tick (after review)", engine.dispatch_followups(w.conn, w.clock, MAILBOX))
    res, tok = recover_once(w, "W2")
    step("outbox worker delivers the #1041 reminder", f"{res} token={tok}")
    ev["snapshots"]["5_after_human_review"] = w.snapshot()

    ev["trace"] = trace_event(w, "evt-B-201")
    ev["world"] = w
    return ev


def trace_event(w, event_id):
    c = w.conn
    e = dict(c.execute("SELECT * FROM inbound_events WHERE event_id=?", (event_id,)).fetchone())
    job = dict(c.execute("SELECT * FROM jobs WHERE sm8_uuid=?", (e["sm8_uuid"],)).fetchone())
    key_prefix = f"confirm:{e['sm8_uuid']}:"
    ob = dict(c.execute("SELECT * FROM outbox WHERE idempotency_key LIKE ? AND sm8_uuid=?",
                        (key_prefix + "%", e["sm8_uuid"])).fetchone())
    attempts = [dict(r) for r in c.execute("SELECT worker, fencing_token, at, step, result FROM outbox_attempts "
                                           "WHERE outbox_id=? ORDER BY id", (ob["id"],))]
    dupes = [dict(r) for r in c.execute("SELECT * FROM inbound_events WHERE dedupe_key=? AND outcome='duplicate'",
                                        (e["dedupe_key"],))]
    msg = next((m for m in w.ghl.messages if m["id"] == ob["provider_message_id"]), None)
    return dict(
        source_event=e, duplicates_rejected=dupes,
        stored_job_now=job, outbox_row=ob, attempts=attempts,
        destination=dict(ghl_message={k: v for k, v in msg.items() if not k.startswith("_")} if msg else None,
                         ghl_messages_for_body=len([m for m in w.ghl.messages if m["body"] == ob["body"]])),
    )


# ===================================================================== variants

def _single_confirmation_world(db_path, lag):
    w = World(db_path, search_lag_minutes=lag, log=lambda *_: None)
    w.seed()
    w.at(1)
    new = w.sm8.update_job(JOB_B, actor="office", start_at="2026-10-21T00:40:00Z")
    engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                            dict(event_id="evt-1", object_uuid=JOB_B, edit_date=new["edit_date"]))
    return w


def run_crash_point(db_path, crash, lag=5):
    """Crash at a given point on first send, then recover on a timeline."""
    w = _single_confirmation_world(db_path, lag)
    steps = []
    w.at(2)
    if crash == "after_send":
        steps.append(("T+2 send", crash_run(w, "W1", "after_send")))
    else:
        steps.append(("T+2 send", crash_run(w, "W1", crash)))
    for minute in (4, 18, 45):
        w.at(minute)
        res, tok = recover_once(w, "W2")
        steps.append((f"T+{minute} recover", "nothing to claim" if res is None else f"{res} (token {tok})"))
    w.at(50)
    steps.append(("T+50 duplicate sweep", engine.reconcile_duplicates(w.conn, w.clock, w.ghl) or "none"))
    row = dict(w.conn.execute("SELECT * FROM outbox").fetchone())
    reviews = [dict(r) for r in w.conn.execute("SELECT kind, subject, evidence, status FROM review_tasks")]
    attempts = [dict(r) for r in w.conn.execute("SELECT worker, fencing_token, at, step, result "
                                                "FROM outbox_attempts ORDER BY id")]
    w.conn.close()
    return dict(steps=steps, outbox=row, ghl_messages=len(w.ghl.delivered()), reviews=reviews,
                attempts=attempts)


def run_ambiguous(db_path):
    """Same reply, but sent as a fresh email with no thread reference."""
    w = World(db_path, log=lambda *_: None)
    w.seed()
    w.at(5)
    engine.sync_mailbox(w.conn, w.clock, w.outlook, MAILBOX)
    w.outlook.receive(CUSTOMER["email"], "booking", "Cancel that one, keep the other.")
    out = engine.process_inbound_emails(w.conn, w.clock, w.ai)
    task = dict(w.conn.execute("SELECT * FROM review_tasks").fetchone())
    paused = [dict(r) for r in w.conn.execute("SELECT sm8_uuid, status FROM followups WHERE status='paused'")]
    w.conn.close()
    return dict(result=out, task_evidence=json.loads(task["evidence"]), paused=paused,
                sm8_writes=list(w.sm8.write_log))


def run_claim_stress(db_path, n=8):
    w = _single_confirmation_world(db_path, 5)
    results = race_claim(w, [f"W{i}" for i in range(1, n + 1)])
    w.conn.close()
    return dict(winners=[k for k, v in results.items() if v], losers=[k for k, v in results.items() if not v])


__all__ = ["run_main", "run_crash_point", "run_ambiguous", "run_claim_stress", "JOB_A", "JOB_B", "iso"]
