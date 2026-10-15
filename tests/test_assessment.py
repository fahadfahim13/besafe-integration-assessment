"""Acceptance tests for the BeSafe assessment scenario. Run: python -m unittest -v"""

import json
import os
import shutil
import tempfile
import threading
import unittest

from besafe_sim import engine, scenario
from besafe_sim.scenario import JOB_A, JOB_B


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def path(self, name):
        return os.path.join(self.dir, name)


class MainScenario(Base):
    @classmethod
    def setUpClass(cls):
        cls._dir = tempfile.mkdtemp()
        cls.ev = scenario.run_main(os.path.join(cls._dir, "main.db"), log=lambda *_: None)
        cls.w = cls.ev["world"]
        cls.steps = {s["step"]: s["result"] for s in cls.ev["steps"]}

    @classmethod
    def tearDownClass(cls):
        cls.w.conn.close()
        shutil.rmtree(cls._dir, ignore_errors=True)

    def outcomes(self):
        return {r["event_id"]: r["outcome"] for r in self.w.conn.execute("SELECT * FROM inbound_events")}

    # 1. job matching, stale + duplicate events
    def test_one_contact_two_opportunities(self):
        self.assertEqual(len(self.w.ghl.contacts), 1)
        self.assertEqual({o["sm8_uuid"] for o in self.w.ghl.opportunities.values()}, {JOB_A, JOB_B})

    def test_reschedule_applied_stale_and_duplicate_refused(self):
        o = self.outcomes()
        self.assertEqual(o["evt-B-201"], "applied")
        self.assertEqual(o["evt-B-200"], "stale")
        dupes = [r["event_id"] for r in
                 self.w.conn.execute("SELECT * FROM inbound_events WHERE outcome='duplicate'")]
        self.assertEqual(dupes, ["evt-B-201-redelivery"])

    def test_job_a_untouched_by_job_b_events(self):
        snap = self.ev["snapshots"]["4_after_automated_recovery"]
        a = next(j for j in snap["store_jobs"] if j["job_number"] == "1041")
        self.assertEqual((a["status"], a["start_at"]), ("Scheduled", "2026-10-21T00:45:00Z"))

    # 2/3. uncertain delivery, simultaneous workers, fencing
    def test_exactly_one_confirmation_delivered(self):
        confirmations = [m for m in self.w.ghl.messages if "is confirmed" in m["body"]]
        self.assertEqual(len(confirmations), 1)
        row = self.w.conn.execute("SELECT * FROM outbox").fetchone()
        self.assertEqual(row["status"], "sent")
        self.assertEqual(row["sends"], 1)
        self.assertEqual(row["provider_message_id"], confirmations[0]["id"])

    def test_race_has_exactly_one_winner(self):
        race = self.steps["W2 and W3 race for the expired lease"]
        self.assertEqual(sorted(v.split()[0] for v in race.values()), ["WON", "lost"])

    def test_not_found_inside_window_does_not_resend(self):
        first = next(v for k, v in self.steps.items() if k.endswith("recovers: provider search"))
        self.assertEqual(first, "held")
        self.assertTrue(self.steps["second recovery pass at 00:10 (search still inside lag)"].startswith("held"))

    def test_stale_fencing_token_rejected(self):
        res = next(v for k, v in self.steps.items() if "wakes late" in k)
        self.assertTrue(res.startswith("REJECTED"), res)

    # 4. Outlook recovery and follow-up control
    def test_missed_notification_recovered_by_delta(self):
        self.assertEqual(len(self.w.outlook.dropped_notifications), 1)
        e = self.w.conn.execute("SELECT * FROM inbound_emails").fetchone()
        self.assertEqual(e["ingested_via"], "delta")

    def test_followups_held_while_mailbox_stale(self):
        held = [s for s in self.ev["steps"] if s["step"].startswith("follow-up dispatcher tick (reminder")]
        for s in held:
            self.assertTrue(all(r.startswith("held") for _, r in s["result"]))
        snap = self.ev["snapshots"]["3_after_failures_all"]
        self.assertEqual(len(snap["ghl"]["messages"]), 1)  # only the confirmation

    def test_reply_pauses_followups_for_both_jobs(self):
        snap = self.ev["snapshots"]["4_after_automated_recovery"]
        open_f = [f for f in snap["followups"] if f["status"] in ("queued", "paused")]
        self.assertTrue(open_f)
        self.assertTrue(all(f["status"] == "paused" for f in open_f))
        self.assertEqual({f["job_number"] for f in open_f}, {"1041", "1042"})

    def test_reply_ingested_but_unprocessed_still_holds_followups(self):
        res = self.steps["follow-up dispatcher tick (mailbox fresh, reply not yet processed)"]
        self.assertTrue(res)
        self.assertTrue(all(r == "held: unprocessed reply" for _, r in res))

    # 5. AI validation and human review
    def test_ai_both_cancelled_is_rejected(self):
        x = self.w.conn.execute("SELECT * FROM ai_extractions").fetchone()
        self.assertGreaterEqual(x["confidence"], 0.9)
        self.assertEqual(x["validation"], "rejected")
        self.assertIn("contradiction", x["reasons"])

    def test_nothing_cancelled_before_human_approval(self):
        snap = self.ev["snapshots"]["4_after_automated_recovery"]
        self.assertTrue(all(j["status"] == "Scheduled" for j in snap["servicem8_jobs"]))
        self.assertEqual([t["status"] for t in snap["review_tasks"]], ["open"])

    def test_final_destination_state(self):
        self.assertEqual(self.w.sm8.jobs[JOB_A]["status"], "Scheduled")
        self.assertEqual(self.w.sm8.jobs[JOB_B]["status"], "Cancelled")
        stages = {o["sm8_uuid"]: o["stage"] for o in self.w.ghl.opportunities.values()}
        self.assertEqual(stages, {JOB_A: "Scheduled", JOB_B: "Cancelled"})
        f = {(r["job_number"], r["status"]) for r in self.ev["snapshots"]["5_after_human_review"]["followups"]}
        self.assertIn(("1041", "dispatched"), f)
        self.assertNotIn(("1042", "dispatched"), f)
        reminders = [m for m in self.w.ghl.messages if "reminder" in m["body"]]
        self.assertEqual(len(reminders), 1)
        self.assertIn("#1041", reminders[0]["body"])

    def test_trace_links_event_to_destination(self):
        t = self.ev["trace"]
        self.assertEqual(t["source_event"]["outcome"], "applied")
        self.assertEqual(t["destination"]["ghl_messages_for_body"], 1)
        self.assertEqual(t["outbox_row"]["provider_message_id"], t["destination"]["ghl_message"]["id"])


class CrashPoints(Base):
    def test_crash_before_external_write_resends_once_after_window(self):
        r = scenario.run_crash_point(self.path("cb.db"), "before_send")
        self.assertEqual(r["ghl_messages"], 1)
        self.assertEqual(r["outbox"]["status"], "sent")
        self.assertEqual(r["outbox"]["sends"], 2)  # attempt 1 never reached GHL
        self.assertTrue(any(a["step"] == "resend_permitted" for a in r["attempts"]))

    def test_crash_after_external_write_reconciles_without_resend(self):
        r = scenario.run_crash_point(self.path("ca.db"), "after_send")
        self.assertEqual(r["ghl_messages"], 1)
        self.assertEqual(r["outbox"]["sends"], 1)
        self.assertIn("reconciled", r["outbox"]["resolution"])

    def test_timeout_before_accept_resends_after_window(self):
        w = scenario._single_confirmation_world(self.path("tb.db"), 5)
        self.addCleanup(w.conn.close)
        w.at(2)
        w.ghl.fail_next_send = "timeout_before_accept"
        self.assertEqual(scenario.recover_once(w, "W1")[0], "uncertain")
        w.at(18)
        self.assertEqual(scenario.recover_once(w, "W2")[0], "sent")
        self.assertEqual(len(w.ghl.messages), 1)

    def test_lag_beyond_window_is_detected_not_hidden(self):
        r = scenario.run_crash_point(self.path("lag.db"), "after_send", lag=30)
        self.assertEqual(r["ghl_messages"], 2)
        self.assertEqual([x["kind"] for x in r["reviews"]], ["duplicate_delivered"])
        ev = json.loads(r["reviews"][0]["evidence"])
        self.assertEqual(len(ev["provider_message_ids"]), 2)

    def test_no_resend_when_job_changed_meanwhile(self):
        w = scenario._single_confirmation_world(self.path("sup.db"), 5)
        self.addCleanup(w.conn.close)
        w.at(2)
        w.ghl.fail_next_send = "timeout_before_accept"
        scenario.recover_once(w, "W1")
        w.at(5)
        new = w.sm8.update_job(JOB_B, actor="office", status="Cancelled")
        engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                                dict(event_id="evt-x", object_uuid=JOB_B, edit_date=new["edit_date"]))
        w.at(18)
        self.assertEqual(scenario.recover_once(w, "W2")[0], "superseded")
        self.assertEqual(len(w.ghl.messages), 0)


class Concurrency(Base):
    def test_eight_simultaneous_workers_one_winner(self):
        for i in range(5):
            r = scenario.run_claim_stress(self.path(f"s{i}.db"), n=8)
            self.assertEqual(len(r["winners"]), 1)
            self.assertEqual(len(r["losers"]), 7)


class Ambiguity(Base):
    def test_no_thread_reference_stays_ambiguous(self):
        r = scenario.run_ambiguous(self.path("amb.db"))
        self.assertTrue(str(r["task_evidence"]["suggestion"]).startswith("AMBIGUOUS"))
        self.assertEqual(len(r["paused"]), 2)
        self.assertEqual(r["sm8_writes"], [])

    def test_validator_accepts_consistent_single_cancel(self):
        cands = [dict(job_number="1041"), dict(job_number="1042")]
        verdict, reasons = engine.validate_extraction(
            "Cancel that one, keep the other.",
            dict(actions=[dict(job_number="1042", action="cancel"), dict(job_number="1041", action="keep")],
                 confidence=0.9), cands)
        self.assertEqual(verdict, "accepted_pending_approval")
        self.assertTrue(any(r.startswith("policy") for r in reasons))

    def test_validator_rejects_unknown_job(self):
        verdict, reasons = engine.validate_extraction(
            "cancel", dict(actions=[dict(job_number="9999", action="cancel")], confidence=0.99),
            [dict(job_number="1041")])
        self.assertEqual(verdict, "rejected")



class ReviewFindings(Base):
    """Regression tests for issues found in independent review."""

    def _world(self, name, lag=5):
        w = scenario._single_confirmation_world(self.path(name), lag)
        self.addCleanup(w.conn.close)
        return w

    def test_cancellation_after_claim_cannot_revive_message(self):
        w = self._world("p1.db")
        w.at(2)
        conn = w.worker_conn()
        row = engine.claim_next(conn, w.clock, "W1")  # claimed, not yet sent
        new = w.sm8.update_job(JOB_B, actor="office", status="Cancelled")
        engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                                dict(event_id="evt-c", object_uuid=JOB_B, edit_date=new["edit_date"]))
        with self.assertRaises(engine.StaleLease):
            engine.process(conn, w.clock, w.sm8, w.ghl, "W1", row)
        conn.close()
        self.assertEqual(w.conn.execute("SELECT status FROM outbox").fetchone()[0], "superseded")
        self.assertEqual(len(w.ghl.messages), 0)

    def test_first_send_rechecks_servicem8(self):
        w = self._world("p1b.db")
        w.at(2)
        w.sm8.update_job(JOB_B, actor="office", status="Cancelled")  # webhook not yet received
        self.assertEqual(scenario.recover_once(w, "W1")[0], "superseded")
        self.assertEqual(len(w.ghl.messages), 0)

    def test_reschedule_back_to_previous_time_confirms_again(self):
        w = self._world("p2.db")
        w.at(2)
        self.assertEqual(scenario.recover_once(w, "W1")[0], "sent")  # confirmation for 00:40
        for minute, start in ((3, "2026-10-22T03:00:00Z"), (4, "2026-10-21T00:40:00Z")):
            w.at(minute)
            new = w.sm8.update_job(JOB_B, actor="office", start_at=start)
            engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                                    dict(event_id=f"evt-{minute}", object_uuid=JOB_B, edit_date=new["edit_date"]))
        w.at(5)
        self.assertEqual(scenario.recover_once(w, "W1")[0], "sent")
        statuses = [r[0] for r in w.conn.execute("SELECT status FROM outbox ORDER BY id")]
        self.assertEqual(statuses, ["sent", "superseded", "sent"])
        live = w.conn.execute("SELECT status FROM followups WHERE sm8_uuid=? AND due_at='2026-10-20T00:40:00Z'",
                              (JOB_B,)).fetchone()[0]
        self.assertEqual(live, "queued")

    def test_event_for_unknown_job_is_recorded_not_crashed(self):
        w = self._world("p3.db")
        out = engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                                      dict(event_id="evt-u", object_uuid="new-job", edit_date="2026-10-20T00:09:00Z"))
        self.assertEqual(out, "unknown_job")
        self.assertEqual(w.conn.execute("SELECT kind FROM review_tasks").fetchone()[0], "unknown_job")

    def test_same_second_edit_is_still_applied(self):
        w = self._world("p8.db")
        w.at(2)
        first = w.sm8.update_job(JOB_B, actor="office", start_at="2026-10-23T01:00:00Z")
        engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                                dict(event_id="e1", object_uuid=JOB_B, edit_date=first["edit_date"]))
        second = w.sm8.update_job(JOB_B, actor="office", start_at="2026-10-23T02:00:00Z")  # same second
        self.assertEqual(first["edit_date"], second["edit_date"])
        out = engine.ingest_sm8_event(w.conn, w.clock, w.sm8, w.ghl,
                                      dict(event_id="e2", object_uuid=JOB_B, edit_date=second["edit_date"]))
        self.assertEqual(out, "applied")
        self.assertEqual(w.conn.execute("SELECT start_at FROM jobs WHERE sm8_uuid=?", (JOB_B,)).fetchone()[0],
                         "2026-10-23T02:00:00Z")

    def test_review_cannot_be_resolved_twice_or_outside_candidates(self):
        ev = scenario.run_main(self.path("p6.db"), log=lambda *_: None)
        w = ev["world"]
        tid = w.conn.execute("SELECT id FROM review_tasks WHERE kind='customer_instruction'").fetchone()[0]
        with self.assertRaises(engine.ReviewError):
            engine.resolve_instruction(w.conn, w.clock, w.sm8, tid, "x", cancel=[JOB_B], keep=[JOB_A])
        with self.assertRaises(engine.ReviewError):
            engine.resolve_instruction(w.conn, w.clock, w.sm8, tid, "x", cancel=["other-job"], keep=[])
        self.assertEqual(sum(1 for x in w.sm8.write_log if x["fields"].get("status") == "Cancelled"), 1)
        w.conn.close()

    def test_malformed_ai_output_is_rejected_not_crashed(self):
        for bad in (None, "cancel", dict(actions="cancel"), dict(actions=["x"]), dict(actions=[])):
            self.assertEqual(engine.validate_extraction("Cancel that one", bad, [dict(job_number="1")])[0],
                             "rejected", bad)

    def test_concurrent_process_sends_exactly_once(self):
        for i in range(5):
            w = self._world(f"p12_{i}.db")
            w.at(2)
            out = {}
            barrier = threading.Barrier(8)

            def run(name):
                barrier.wait()
                out[name] = scenario.recover_once(w, name)[0]

            ts = [threading.Thread(target=run, args=(f"W{n}",)) for n in range(8)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
            self.assertEqual(len(w.ghl.messages), 1)
            self.assertEqual(sorted(v for v in out.values() if v), ["sent"])

    def test_reminder_goes_through_outbox_once(self):
        ev = scenario.run_main(self.path("rem.db"), log=lambda *_: None)
        rows = ev["world"].conn.execute("SELECT status FROM outbox WHERE kind='reminder'").fetchall()
        self.assertEqual([r[0] for r in rows], ["sent"])
        ev["world"].conn.close()


if __name__ == "__main__":
    unittest.main()
