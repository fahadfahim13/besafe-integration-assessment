"""In-memory stand-ins for ServiceM8, GoHighLevel, Microsoft Graph (Outlook)
and the AI extractor.

These are SIMULATED. They reproduce the provider behaviours the assessment
cares about, not the full APIs:
- ServiceM8: jobs carry an edit_date that changes on every edit; webhooks may
  be delivered late, duplicated or out of order.
- GHL: send-message has no idempotency key; a request can be accepted while
  the HTTP response times out; message search is eventually consistent.
- Graph: change notifications can be missed; the delta query returns every
  message since the last delta link regardless of notifications.
- AI: returns a structured extraction with a confidence score that can be
  confidently wrong.
"""

import copy
import itertools

from .clock import iso


class ProviderTimeout(Exception):
    """The request may or may not have been applied by the provider."""


class SimulatedCrash(BaseException):
    """Process death. BaseException so ordinary `except Exception` handlers
    in worker code cannot swallow it, exactly like SIGKILL cannot be caught."""


class ServiceM8:
    def __init__(self, clock):
        self.clock = clock
        self.jobs = {}
        self.write_log = []

    def seed_job(self, uuid, **fields):
        self.jobs[uuid] = dict(uuid=uuid, edit_date=self.clock.now_iso(), **fields)

    def get_job(self, uuid):
        return copy.deepcopy(self.jobs[uuid])

    def update_job(self, uuid, actor, **fields):
        job = self.jobs[uuid]
        job.update(fields)
        job["edit_date"] = self.clock.now_iso()
        self.write_log.append(dict(at=self.clock.now_iso(), actor=actor, uuid=uuid, fields=fields))
        return copy.deepcopy(job)


class GHL:
    def __init__(self, clock, search_lag_minutes=5):
        self.clock = clock
        self.search_lag_minutes = search_lag_minutes
        self.contacts = {}
        self.opportunities = {}
        self.messages = []
        self._ids = itertools.count(1)
        self.fail_next_send = None  # None | "timeout_after_accept" | "timeout_before_accept"

    def _id(self, prefix):
        return f"{prefix}_{next(self._ids):04d}"

    # Search-before-create keyed on email OR phone: both jobs share the
    # customer, so both map to one contact.
    def upsert_contact(self, email, phone, name):
        for c in self.contacts.values():
            if c["email"] == email or c["phone"] == phone:
                return c["id"]
        cid = self._id("ghl_contact")
        self.contacts[cid] = dict(id=cid, email=email, phone=phone, name=name)
        return cid

    # One opportunity per ServiceM8 job, keyed on the job UUID custom field.
    def upsert_opportunity(self, contact_id, sm8_uuid, name, stage):
        for o in self.opportunities.values():
            if o["sm8_uuid"] == sm8_uuid:
                o["stage"] = stage
                return o["id"]
        oid = self._id("ghl_opp")
        self.opportunities[oid] = dict(id=oid, contact_id=contact_id, sm8_uuid=sm8_uuid, name=name, stage=stage)
        return oid

    def send_message(self, contact_id, body):
        mode, self.fail_next_send = self.fail_next_send, None
        if mode == "timeout_before_accept":
            raise ProviderTimeout("POST /conversations/messages: read timeout (request not applied)")
        mid = self._id("ghl_msg")
        self.messages.append(dict(id=mid, contact_id=contact_id, body=body, created_at=self.clock.now_iso(),
                                  _created=self.clock.now()))
        if mode == "timeout_after_accept":
            raise ProviderTimeout("POST /conversations/messages: read timeout after 30s (request was accepted)")
        return dict(messageId=mid)

    def search_messages(self, contact_id, body):
        """Eventually consistent: a message becomes searchable only after
        search_lag_minutes."""
        now = self.clock.now()
        return [
            dict(id=m["id"], created_at=m["created_at"])
            for m in self.messages
            if m["contact_id"] == contact_id and m["body"] == body
            and (now - m["_created"]).total_seconds() >= self.search_lag_minutes * 60
        ]

    def delivered(self, contact_id=None):
        return [m for m in self.messages if contact_id is None or m["contact_id"] == contact_id]


class Outlook:
    """Shared mailbox. Notifications go to a webhook handler unless an outage
    is active; the delta query always sees every message."""

    def __init__(self, clock):
        self.clock = clock
        self.messages = []
        self.notifications_up = True
        self.dropped_notifications = []
        self.webhook = None
        self._ids = itertools.count(1)

    def record_outbound(self, internet_message_id, subject):
        self.messages.append(dict(graph_id=f"AAMk_out_{next(self._ids):03d}", direction="out",
                                  internet_message_id=internet_message_id, subject=subject,
                                  received_at=self.clock.now_iso()))

    def receive(self, from_addr, subject, body, in_reply_to=None):
        gid = f"AAMk_in_{next(self._ids):03d}"
        msg = dict(graph_id=gid, direction="in", from_addr=from_addr, subject=subject, body=body,
                   in_reply_to=in_reply_to, received_at=self.clock.now_iso())
        self.messages.append(msg)
        if self.notifications_up and self.webhook:
            self.webhook(gid)
        else:
            self.dropped_notifications.append(dict(graph_id=gid, at=self.clock.now_iso()))
        return gid

    def get_message(self, graph_id):
        return copy.deepcopy(next(m for m in self.messages if m["graph_id"] == graph_id))

    def delta(self, delta_link):
        start = int(delta_link.split("=")[1]) if delta_link else 0
        new = [copy.deepcopy(m) for m in self.messages[start:] if m["direction"] == "in"]
        return new, f"deltatoken={len(self.messages)}"


class AIExtractor:
    """Deliberately reproduces the scenario's failure: labels BOTH jobs
    cancelled with high confidence."""

    model = "extractor-sim-v1"

    def extract(self, body, candidate_jobs):
        return dict(
            intent="cancel",
            actions=[dict(job_number=j["job_number"], action="cancel") for j in candidate_jobs],
            confidence=0.96,
        )


def snapshot_ghl(ghl):
    # Deep copies: a snapshot must not change when the provider changes later.
    return dict(
        contacts=copy.deepcopy(list(ghl.contacts.values())),
        opportunities=copy.deepcopy(list(ghl.opportunities.values())),
        messages=[{k: v for k, v in m.items() if not k.startswith("_")} for m in ghl.messages],
    )


__all__ = ["ServiceM8", "GHL", "Outlook", "AIExtractor", "ProviderTimeout", "SimulatedCrash", "iso",
           "snapshot_ghl"]
