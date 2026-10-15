# BeSafe GHL integration: synthetic failure-and-recovery assessment

A runnable simulation of the assessment scenario. It covers two ServiceM8 jobs for one customer, late and duplicate events, a GHL timeout followed by a worker crash, racing workers, a missed Outlook reply, and a confidently wrong AI extraction. It records the state before processing, after the failures and after recovery.

**Live:** https://besafe-assessment.vercel.app — the recorded walkthrough, every snapshot, the event trace and the crash-point variants. The **Run tests** and **Re-run demo** buttons execute this code on the server.

All data is synthetic. No credentials, no production systems, no paid services and no former-client information are used.

```
python -m unittest discover -s tests -t . -v    # 37 acceptance tests
python run_demo.py                               # walkthrough + evidence/ (exit 0 = all checks pass)
python viewer.py                                 # browse the evidence at http://127.0.0.1:8765
```

Requires Python 3.11+ (standard library only; SQLite 3.35+ for `UPDATE ... RETURNING`).

## What is working, simulated or proposed

| Part | Label | Notes |
|---|---|---|
| Worker logic: event reconciliation, version guard, outbox, leases, fencing, recovery, resend rule, follow-up gate, mailbox recovery, AI validation, review flow | **Working** | `besafe_sim/engine.py`. Runs against a real SQLite database. Lease claims and full claim-and-send are exercised with 8 real concurrent threads (tests). The paused-worker late write in the walkthrough is a deterministic replay of that interleaving, not a live race. |
| ServiceM8, GHL, Microsoft Graph (Outlook), AI model | **Simulated** | `besafe_sim/providers.py`. In-memory stand-ins that reproduce the behaviours under test: GHL accepting a request whose response times out, eventually consistent search, Graph notifications that can be dropped, an AI that labels both jobs cancelled at 0.96 confidence. |
| Time | **Simulated** | A deterministic clock, so a 60-minute incident replays in about a second and gives the same evidence on every run. |
| Production deployment against live APIs, Postgres, scheduler, alerting | **Proposed** | See "Production mapping". Not demonstrated here. |
| Integration writing to ServiceM8 (the cancellation after human approval) | **Proposed / simulated** | My existing production ServiceM8 work reads from ServiceM8 and writes to GHL. Writing back to ServiceM8 is shown here only against the simulator. |

## Scenario coverage

| Requirement | Mechanism | Where |
|---|---|---|
| Job matching | Job identity = ServiceM8 job UUID. Email and phone resolve the **customer** (one GHL contact); each job is its own opportunity keyed on the UUID. Events for #1042 never touch #1041. | `engine.import_customer_jobs`, `_apply_job` |
| Duplicate events | Every delivery is reconciled against ServiceM8's current record (edit_date + content hash of status and start time). A redelivery finds that version already applied and is recorded `duplicate`, a no-op. Replaying any event any number of times is safe. An edit in the same second as a previous one is still applied, because its content differs. | `ingest_sm8_event` |
| Stale updates | Version guard: an event older than the stored `sm8_edit_date` is recorded `stale` and not applied. For newer events the current record is re-read from ServiceM8, so the payload is never trusted for field values. An event for a job not yet imported is recorded `unknown_job` and queued for review instead of failing the webhook. | `ingest_sm8_event` |
| Simultaneous workers | Claim is one conditional `UPDATE ... WHERE lease expired ... RETURNING`; only one connection can match. Every claim increments a **fencing token**, and every outcome write is `WHERE fencing_token = ? AND status IN (expected)`. A worker that lost its lease, or whose row was superseded meanwhile (for example, the job was cancelled after it claimed), cannot overwrite state. | `claim_next`, `fenced_update` |
| Uncertain delivery | Before **any** send, ServiceM8 is re-read: job not cancelled, same start time. Intent (`in_flight`) is committed **before** the GHL call. A timeout, or an `in_flight` row with an expired lease, becomes `uncertain`. It is never resent blind. | `_send`, `_recover` |
| When resending is permitted | **All** must hold: (a) worker holds a current lease/token; (b) a provider search that **started** at or after `first_send_at + 15 min` (search-consistency window) finds no match; (c) the message is still true per a fresh ServiceM8 read; (d) no previous resend. Otherwise: hold with backoff, close as superseded, or open a review task. | `_recover` |
| Missed Outlook reply | Graph notifications are a latency hint only. A delta query with a stored delta link is the completeness guarantee. Webhook and delta paths are idempotent with each other (`graph_id` primary key). | `sync_mailbox`, `ingest_mail` |
| Queued follow-ups | (1) No dispatch while the mailbox watermark is older than 5 minutes. (2) No dispatch while the customer has a reply that is ingested but not yet processed or reviewed. (3) Processing a reply pauses follow-ups for **every** job it could refer to. (4) Reminders are delivered through the same outbox (idempotency key, lease, fencing, ServiceM8 re-check), so they get the same crash protection as confirmations, including the same provider-lag limit described below. (5) After review, follow-ups for the jobs being kept resume once the customer has no reply left open; cancelled jobs' follow-ups are cancelled. | `dispatch_followups`, `process_inbound_emails`, `resolve_instruction` |
| Ambiguous instruction | "Cancel that one, keep the other" is resolved only through a deterministic signal: the reply's `In-Reply-To` matches the booking email for #1042. Even then it is a **suggestion**. With no thread reference (variant V4) it stays `AMBIGUOUS` and the reviewer is told to ask the customer. | `process_inbound_emails` |
| AI validation | Type/schema check (malformed output or a model error is a rejection, never a crash), reference check (jobs must belong to this customer), contradiction check ("keep the other" vs cancelling every job), count check ("that one" means exactly one cancellation). Cancellation is destructive, so it always needs human approval whatever the confidence. | `validate_extraction` |
| Human review | The task is claimed `open -> resolving` with a conditional update, so a second reviewer or a double click cannot act on it; a crash mid-resolution can be re-run by the same reviewer. The decision must reference only the task's own candidate jobs. The ServiceM8 write sets absolute state, so it is safe to repeat; the store learns about it through the normal webhook. | `resolve_instruction` |
| Destination evidence | Snapshots of ServiceM8, the store and GHL at each stage; one event traced end to end; generated results table. | `evidence/` |

## Crash immediately before vs immediately after the external write

From the database's point of view the two cases look identical. The row is `in_flight` with an expired lease, because the worker died between committing intent and committing the outcome. The design does not guess which case it is. The provider answers that question:

- **Crash immediately before the write** (V1). GHL never received the request. Recovery marks it `uncertain` and searches GHL: not found. It holds until the 15-minute window passes, searches again (still not found), and only then resends. Result: **1 message**, `sends=2` (the first send was claimed but never reached GHL).
- **Crash immediately after the write** (V2, and the main scenario). GHL accepted the message. The first search can miss it (search lags 5 minutes), so the row is held, not resent. A later search finds it and the row is reconciled as `sent` with the provider message id. Result: **1 message**, `sends=1`, no resend.

## The failure this design cannot resolve automatically

**GHL's send-message endpoint has no idempotency key.** If GHL accepted a message, the response was lost, and GHL's search lag goes beyond the consistency window the system relies on (V3: 30 minutes against a 15-minute window), the search after the window wrongly says "not found". The worker then resends and the customer gets two SMS. No client-side design can prevent this without a provider-side idempotency key.

What the system does:
- It **detects** the duplicate with a post-delivery sweep and opens a `duplicate_delivered` review task. It does not hide it.
- The resend budget is one, so a lagging provider cannot cause a loop.

Evidence the reviewer gets (in the task, see `evidence/variants.json`):
- the idempotency key and outbox id;
- both GHL message ids and their creation times;
- the full attempt log: each worker, fencing token, timestamp, step and result, including the timeout text and the search results that justified the resend.

The reviewer then checks the GHL conversation and the carrier delivery report, and decides whether to send the customer an apology or correction.

Tuning lever: lengthen the consistency window for customer-facing SMS, or set `MAX_SENDS = 1`, which turns every unconfirmable send into a human review instead of a possible duplicate.

## Production mapping (proposed)

| Simulation | Production |
|---|---|
| SQLite, `BEGIN IMMEDIATE`, conditional `UPDATE ... RETURNING` | Postgres; the same conditional update (or `FOR UPDATE SKIP LOCKED`), unique indexes unchanged. The ServiceM8 read moves outside the write transaction, with an optimistic version check on update |
| ISO-8601 UTC strings compared as text | ServiceM8 `edit_date` normalised to UTC in one fixed format on ingest (or stored as `timestamptz`) before any comparison |
| GHL opportunity stage / ServiceM8 status writes | Absolute-state upserts keyed on the job UUID after the local commit; a periodic re-sync of the same function repairs a failed call |
| `ServiceM8.get_job` | ServiceM8 REST job fetch by UUID after each webhook; `edit_date` as the version |
| `GHL.send_message` / `search_messages` | GHL Conversations API send, plus reading the contact's conversation messages (body + time window) for reconciliation |
| `Outlook.delta` / webhook | Microsoft Graph change notifications for latency; `messages/delta` on the shared mailbox every few minutes for completeness; Graph lifecycle notifications trigger an immediate delta run |
| Review tasks | GHL task + internal review queue with the evidence JSON attached |
| Simulated clock | Real scheduler; window, backoff and staleness limits as configuration |

## Files

- `besafe_sim/engine.py`: the worker (rules R1-R8 documented at the top)
- `besafe_sim/db.py`: schema; every guarantee is a database constraint or conditional write
- `besafe_sim/providers.py`: simulated ServiceM8 / GHL / Outlook / AI
- `besafe_sim/scenario.py`: the main scenario and variants V1-V5
- `run_demo.py`: walkthrough + generated `evidence/`
- `api/index.py`, `vercel.json`: hosted copy of the viewer (same `Handler`; live runs write to `/tmp`)
- `viewer.py`: local evidence viewer (results, timeline, before/after snapshots with change highlighting, event trace, crash-point variants; buttons re-run the demo and the tests live)
- `tests/test_assessment.py`: 37 acceptance tests, including a regression test for each issue found in an independent review of the first version
- `evidence/`: `run-output.txt`, `results.md`, `trace-evt-B-201.json`, `snapshots.json`, `variants.json`, `main-scenario.sqlite`

## Authorship

Designed and implemented by Fahim Fahad, with AI coding assistance (Claude Code) as the brief permits. I'm responsible for the design and can explain or change any part of it live.
