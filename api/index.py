"""Vercel entry point: serves the same evidence viewer as `python viewer.py`."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from viewer import Handler  # noqa: E402


class handler(Handler):  # Vercel's Python runtime looks for a top-level `handler`
    pass
