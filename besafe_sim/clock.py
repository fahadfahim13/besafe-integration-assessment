"""Deterministic simulation clock so every run produces identical evidence."""

from datetime import datetime, timedelta, timezone


def iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


class SimClock:
    def __init__(self, start=None):
        self._t = start or datetime(2026, 10, 20, 0, 0, 0, tzinfo=timezone.utc)

    def now(self):
        return self._t

    def now_iso(self):
        return iso(self._t)

    def advance(self, **kwargs):
        self._t += timedelta(**kwargs)
        return self._t
