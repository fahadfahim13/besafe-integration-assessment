"""Run the full BeSafe assessment scenario and its variants.

    python run_demo.py            # prints the walkthrough, writes evidence/

Every PASS/FAIL in the results table is computed from the stored state and
the simulated destinations at the end of the run, not written by hand.
"""

import json
import os
import shutil
import sys
import tempfile

from besafe_sim import scenario
from besafe_sim.scenario import JOB_A, JOB_B

HERE = os.path.dirname(os.path.abspath(__file__))
# Overridable so a read-only deployment can write its run to /tmp.
EVIDENCE = os.environ.get("BESAFE_EVIDENCE_DIR") or os.path.join(HERE, "evidence")
OUT = []
UNPROC = "follow-up dispatcher tick (mailbox fresh, reply not yet processed)"


def p(line=""):
    OUT.append(line)
    print(line)


def table(title, rows, cols):
    p(f"\n  {title}")
    if not rows:
        p("    (none)")
        return
    widths = {c: min(max(len(c), *(len(str(r.get(c, ""))) for r in rows)), 64) for c in cols}
    p("    " + " | ".join(c.ljust(widths[c]) for c in cols))
    p("    " + "-+-".join("-" * widths[c] for c in cols))
    for r in rows:
        p("    " + " | ".join(str(r.get(c, ""))[:64].ljust(widths[c]) for c in cols))


def show_snapshot(name, s):
    p("\n" + "=" * 100)
    p(f"SNAPSHOT: {name}   (sim time {s['at']})")
    p("=" * 100)
    table("ServiceM8 (destination, simulated)", s["servicem8_jobs"],
          ["job_number", "status", "start_at", "edit_date"])
    table("Store: jobs", s["store_jobs"], ["job_number", "status", "start_at", "sm8_edit_date"])
    table("Store: inbound_events", s["inbound_events"], ["event_id", "event_edit_date", "outcome", "detail"])
    table("Store: outbox", s["outbox"], ["id", "kind", "status", "sends", "fencing_token", "lease_owner",
                                         "resend_eligible_at", "provider_message_id", "resolution"])
    table("Store: followups", s["followups"], ["id", "job_number", "kind", "due_at", "status", "reason"])
    table("Store: inbound_emails", s["inbound_emails"], ["graph_id", "in_reply_to", "ingested_via", "status"])
    table("Store: ai_extractions", s["ai_extractions"], ["graph_id", "confidence", "validation"])
    table("Store: review_tasks", s["review_tasks"], ["id", "kind", "status", "resolution", "resolved_by"])
    table("GHL opportunities (destination, simulated)", s["ghl"]["opportunities"], ["id", "name", "stage"])
    table("GHL messages (destination, simulated)", s["ghl"]["messages"], ["id", "created_at", "body"])
    table("Outlook dropped notifications", s["outlook_dropped_notifications"], ["graph_id", "at"])


def main():
    if os.path.isdir(EVIDENCE):
        shutil.rmtree(EVIDENCE)
    os.makedirs(EVIDENCE)
    tmp = tempfile.mkdtemp()

    p("BeSafe integration assessment - synthetic run")
    p("All data is synthetic. ServiceM8, GHL, Outlook and the AI model are simulated providers.\n")
    p("TIMELINE")
    ev = scenario.run_main(os.path.join(tmp, "main.db"), log=p)
    w = ev["world"]
    for name, snap in ev["snapshots"].items():
        show_snapshot(name, snap)

    # ---------------------------------------------------- trace
    p("\n" + "=" * 100)
    p("TRACE: evt-B-201 (job #1042 reschedule) from source event to destination")
    p("=" * 100)
    t = ev["trace"]
    se = t["source_event"]
    p(f"  source event   : {se['event_id']}  job={se['sm8_uuid']}  edit_date={se['event_edit_date']}")
    p(f"  dedupe key     : {se['dedupe_key']}  -> outcome={se['outcome']} ({se['detail']})")
    for d in t["duplicates_rejected"]:
        p(f"  duplicate      : {d['event_id']} at {d['received_at']} -> no-op ({d['detail']})")
    ob = t["outbox_row"]
    p(f"  outbox         : id={ob['id']} key={ob['idempotency_key']}")
    p(f"                   status={ob['status']} sends={ob['sends']} fencing_token={ob['fencing_token']}")
    for a in t["attempts"]:
        p(f"  attempt        : {a['at']} {a['worker']:<3} token={a['fencing_token']} {a['step']:<16} {a['result']}")
    dm = t["destination"]["ghl_message"]
    p(f"  destination    : GHL {dm['id']} at {dm['created_at']} (messages with this body: "
      f"{t['destination']['ghl_messages_for_body']})")

    # ---------------------------------------------------- variants
    p("\n" + "=" * 100)
    p("VARIANTS: crash immediately before / after the external write, provider lag beyond window")
    p("=" * 100)
    variants = {
        "V1 crash immediately BEFORE external write": scenario.run_crash_point(os.path.join(tmp, "v1.db"),
                                                                            "before_send"),
        "V2 crash immediately AFTER external write": scenario.run_crash_point(os.path.join(tmp, "v2.db"),
                                                                           "after_send"),
        "V3 provider search lag (30m) exceeds window (15m)": scenario.run_crash_point(
            os.path.join(tmp, "v3.db"), "after_send", lag=30),
    }
    for name, r in variants.items():
        p(f"\n  {name}")
        for label, res in r["steps"]:
            p(f"    {label:<22} {res}")
        p(f"    -> outbox status={r['outbox']['status']} sends={r['outbox']['sends']} "
          f"resolution={r['outbox']['resolution']}; GHL messages delivered={r['ghl_messages']}")
        for rv in r["reviews"]:
            p(f"    -> REVIEW TASK {rv['kind']} ({rv['status']}): {rv['evidence']}")
    amb = scenario.run_ambiguous(os.path.join(tmp, "v4.db"))
    p("\n  V4 same reply with NO thread reference")
    p(f"    suggestion: {amb['task_evidence']['suggestion']}")
    p(f"    follow-ups paused: {len(amb['paused'])}; ServiceM8 writes: {len(amb['sm8_writes'])}")
    stress = [scenario.run_claim_stress(os.path.join(tmp, f"v5_{i}.db"), n=8) for i in range(20)]
    p("\n  V5 eight workers claim the same row simultaneously, 20 rounds")
    p(f"    winners per round: {[len(s['winners']) for s in stress]}")

    # ---------------------------------------------------- results table
    s4 = ev["snapshots"]["4_after_automated_recovery"]
    s5 = ev["snapshots"]["5_after_human_review"]
    outcomes = {r["event_id"]: r["outcome"] for r in s5["inbound_events"]}
    conf = [m for m in w.ghl.messages if "is confirmed" in m["body"]]
    rem = [m for m in w.ghl.messages if "reminder" in m["body"]]
    steps = {s["step"]: s["result"] for s in ev["steps"]}
    race = steps["W2 and W3 race for the expired lease"]
    late = next(v for k, v in steps.items() if "wakes late" in k)
    held = [s["result"] for s in ev["steps"] if s["step"].startswith("follow-up dispatcher tick (reminder")]
    ai = s5["ai_extractions"][0]
    stages = {o["sm8_uuid"]: o["stage"] for o in w.ghl.opportunities.values()}

    checks = [
        ("Job matching", "Same email/phone -> 1 GHL contact; 2 opportunities keyed by ServiceM8 UUID",
         f"{len(w.ghl.contacts)} contact, {len(w.ghl.opportunities)} opportunities",
         len(w.ghl.contacts) == 1 and set(stages) == {JOB_A, JOB_B}, "Working (simulated providers)"),
        ("Reschedule applied", "evt-B-201 applied from ServiceM8's current record", outcomes.get("evt-B-201"),
         outcomes.get("evt-B-201") == "applied", "Working"),
        ("Stale update", "Late older evt-B-200 refused by version guard", outcomes.get("evt-B-200"),
         outcomes.get("evt-B-200") == "stale", "Working"),
        ("Duplicate event", "Redelivered evt-B-201 is a no-op (version + content already applied)",
         outcomes.get("evt-B-201-redelivery"), outcomes.get("evt-B-201-redelivery") == "duplicate", "Working"),
        ("Simultaneous workers", "Exactly one worker wins the lease", str(race),
         sorted(v.split()[0] for v in race.values()) == ["WON", "lost"], "Working (real threads, SQLite)"),
        ("Stress", "1 winner in each of 20 rounds of 8 workers",
         f"{sum(len(s['winners']) == 1 for s in stress)}/20 rounds", all(len(s["winners"]) == 1 for s in stress),
         "Working"),
        ("Fencing", "Paused worker's late write with old token refused", late.split(":")[0],
         late.startswith("REJECTED"), "Working"),
        ("Uncertain delivery", "Timeout + crash -> not resent while search may lag; reconciled from search",
         f"{len(conf)} confirmation delivered, outbox sends={s5['outbox'][0]['sends']}",
         len(conf) == 1 and s5["outbox"][0]["sends"] == 1, "Working (GHL simulated)"),
        ("Resend condition", "Crash before write: resend only after window + negative search",
         f"V1 delivered={variants['V1 crash immediately BEFORE external write']['ghl_messages']}",
         variants["V1 crash immediately BEFORE external write"]["ghl_messages"] == 1, "Working"),
        ("Crash after write", "Reconciled from provider, no resend",
         f"V2 delivered={variants['V2 crash immediately AFTER external write']['ghl_messages']}",
         variants["V2 crash immediately AFTER external write"]["ghl_messages"] == 1, "Working"),
        ("Missed Outlook reply", "Dropped notification recovered by Graph delta poll",
         s5["inbound_emails"][0]["ingested_via"], s5["inbound_emails"][0]["ingested_via"] == "delta",
         "Working (Graph simulated)"),
        ("Follow-up control (outage)", "Reminders held while mailbox watermark is stale",
         str(held), all(all(r.startswith("held") for _, r in h) for h in held), "Working"),
        ("Follow-up control (unprocessed reply)", "Mailbox fresh but reply unprocessed -> still held",
         str(steps[UNPROC]), bool(steps[UNPROC]) and all(r == "held: unprocessed reply" for _, r in steps[UNPROC]),
         "Working"),
        ("Follow-up control (reply)", "Reply pauses follow-ups for BOTH candidate jobs",
         str(sorted({f["job_number"] for f in s4["followups"] if f["status"] == "paused"})),
         {f["job_number"] for f in s4["followups"] if f["status"] == "paused"} == {"1041", "1042"}, "Working"),
        ("AI validation", "High-confidence 'both cancelled' rejected",
         f"confidence {ai['confidence']} -> {ai['validation']}", ai["validation"] == "rejected", "Working"),
        ("Human review", "No cancellation before approval",
         str([j["status"] for j in s4["servicem8_jobs"]]),
         all(j["status"] == "Scheduled" for j in s4["servicem8_jobs"]), "Working"),
        ("Ambiguity", "No thread reference -> ambiguous, no suggestion, no write",
         str(amb["task_evidence"]["suggestion"])[:40],
         str(amb["task_evidence"]["suggestion"]).startswith("AMBIGUOUS") and not amb["sm8_writes"], "Working"),
        ("Destination: ServiceM8", "#1041 Scheduled, #1042 Cancelled",
         f"{w.sm8.jobs[JOB_A]['status']} / {w.sm8.jobs[JOB_B]['status']}",
         (w.sm8.jobs[JOB_A]["status"], w.sm8.jobs[JOB_B]["status"]) == ("Scheduled", "Cancelled"),
         "Simulated destination"),
        ("Destination: GHL", "1 confirmation (#1042) + 1 reminder (#1041); #1042 opp Cancelled",
         f"{len(conf)} conf, {len(rem)} reminder ({'#1041' if rem and '#1041' in rem[0]['body'] else '?'})",
         len(conf) == 1 and len(rem) == 1 and "#1041" in rem[0]["body"] and stages[JOB_B] == "Cancelled",
         "Simulated destination"),
        ("Unresolvable case", "Provider lag beyond window -> duplicate detected + review task",
         f"V3 delivered={variants['V3 provider search lag (30m) exceeds window (15m)']['ghl_messages']}, "
         f"reviews={[x['kind'] for x in variants['V3 provider search lag (30m) exceeds window (15m)']['reviews']]}",
         variants["V3 provider search lag (30m) exceeds window (15m)"]["reviews"][0]["kind"] == "duplicate_delivered",
         "Working: detected, NOT prevented"),
    ]

    p("\n" + "=" * 100)
    p("RESULTS")
    p("=" * 100)
    md = ["| # | Check | Expected | Observed | Result | Status |", "|---|---|---|---|---|---|"]
    for i, (name, exp, obs, ok, status) in enumerate(checks, 1):
        p(f"  {i:>2}. {'PASS' if ok else 'FAIL'}  {name}: {obs}")
        md.append(f"| {i} | {name} | {exp} | {str(obs).replace('|', '/')} | {'PASS' if ok else 'FAIL'} | {status} |")
    passed = sum(1 for c in checks if c[3])
    p(f"\n  {passed}/{len(checks)} checks passed")

    unresolved = [t for t in s5["review_tasks"] if t["status"] == "open"]
    p(f"  Main scenario open review tasks after human review: {len(unresolved)}")

    with open(os.path.join(EVIDENCE, "results.md"), "w", encoding="utf-8") as f:
        f.write("# Results (generated by run_demo.py)\n\n" + "\n".join(md) +
                f"\n\n**{passed}/{len(checks)} checks passed.**\n")
    with open(os.path.join(EVIDENCE, "trace-evt-B-201.json"), "w", encoding="utf-8") as f:
        json.dump(ev["trace"], f, indent=2, default=str)
    with open(os.path.join(EVIDENCE, "steps.json"), "w", encoding="utf-8") as f:
        json.dump(ev["steps"], f, indent=2, default=str)
    with open(os.path.join(EVIDENCE, "results.json"), "w", encoding="utf-8") as f:
        json.dump([dict(check=c[0], expected=c[1], observed=str(c[2]), passed=bool(c[3]), status=c[4])
                   for c in checks], f, indent=2)
    with open(os.path.join(EVIDENCE, "snapshots.json"), "w", encoding="utf-8") as f:
        json.dump(ev["snapshots"], f, indent=2, default=str)
    with open(os.path.join(EVIDENCE, "variants.json"), "w", encoding="utf-8") as f:
        json.dump(dict(variants=variants, ambiguous=amb, stress=stress), f, indent=2, default=str)
    with open(os.path.join(EVIDENCE, "run-output.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(OUT) + "\n")
    w.conn.close()
    shutil.copy(os.path.join(tmp, "main.db"), os.path.join(EVIDENCE, "main-scenario.sqlite"))
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
