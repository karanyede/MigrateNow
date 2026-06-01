"""
Client-side API-call counter and rate-limit header parser.

ServiceNow returns these headers on responses:
    X-RateLimit-Limit  – requests allowed per hour
    X-RateLimit-Reset  – UNIX timestamp when the window resets
    Retry-After        – seconds to wait (on 429 responses)

Because there is *no* ``X-RateLimit-Remaining`` header, we compute an
estimate:  remaining ≈ limit − calls_since_last_reset.

A dedicated ``api_calls`` logger writes every API call to a separate
log file (``logs/api_calls.log``) for easy auditing.
"""

from __future__ import annotations

import time
import threading
import logging
import logging.handlers
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger("sn_migration")

# ── Dedicated API-call logger (file only — no console spam) ──────────
_api_logger = logging.getLogger("api_calls")

def _setup_api_logger() -> None:
    """Create a file handler for API call tracking (audit log only)."""
    if _api_logger.handlers:
        return
    _api_logger.setLevel(logging.INFO)
    # Prevent propagation to root logger (avoids console output)
    _api_logger.propagate = False
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    fh = logging.handlers.RotatingFileHandler(
        log_dir / "api_calls.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter(
        "%(asctime)s │ %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    ))
    _api_logger.addHandler(fh)

_setup_api_logger()


@dataclass
class _InstanceTracker:
    """Mutable state for a single ServiceNow instance."""

    label: str
    calls_total: int = 0
    calls_since_reset: int = 0
    rate_limit: int = 0          # from X-RateLimit-Limit
    rate_reset_ts: float = 0.0   # from X-RateLimit-Reset
    last_retry_after: int = 0    # from Retry-After
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    # ── helpers ──────────────────────────────────────────────────────
    @property
    def estimated_remaining(self) -> int | None:
        """
        Best-effort remaining calls in the current window.
        Returns ``None`` if we never received rate-limit headers.
        """
        if self.rate_limit == 0:
            return None
        # If the window has reset since we last saw the header, restart.
        if time.time() >= self.rate_reset_ts:
            return self.rate_limit
        return max(self.rate_limit - self.calls_since_reset, 0)


class RateTracker:
    """
    Thread-safe tracker for API calls against two ServiceNow instances
    (source and target).

    Usage::

        tracker = RateTracker()

        # after every HTTP response
        tracker.record_call("source", response)

        # at report time
        print(tracker.summary())
    """

    def __init__(self) -> None:
        self._instances: dict[str, _InstanceTracker] = {
            "source": _InstanceTracker(label="Source"),
            "target": _InstanceTracker(label="Target"),
        }

    # ── public API ───────────────────────────────────────────────────

    def record_call(self, instance: str, response=None) -> None:
        """
        Increment the call counter for *instance* (``"source"`` or
        ``"target"``) and parse rate-limit headers from *response*.
        """
        trk = self._instances[instance]
        with trk._lock:
            trk.calls_total += 1
            trk.calls_since_reset += 1

            if response is not None:
                self._parse_headers(trk, response)

            # Audit log (file only — no console output per call)
            status = getattr(response, "status_code", "?") if response else "?"
            remaining = trk.estimated_remaining
            remaining_str = str(remaining) if remaining is not None else "?"
            total_all = sum(t.calls_total for t in self._instances.values())
            _api_logger.info(
                "%s #%d │ HTTP %s │ remaining ~%s │ total(all): %d",
                trk.label, trk.calls_total, status, remaining_str, total_all,
            )

    def get_total_calls(self, instance: str) -> int:
        return self._instances[instance].calls_total

    def get_remaining(self, instance: str) -> int | None:
        return self._instances[instance].estimated_remaining

    def get_rate_limit(self, instance: str) -> int:
        return self._instances[instance].rate_limit

    def summary(self) -> dict:
        """
        Return a JSON-serialisable dict for the final migration report.
        """
        result: dict = {}
        total = 0
        for key, trk in self._instances.items():
            remaining = trk.estimated_remaining
            result[key] = {
                "label": trk.label,
                "calls_made": trk.calls_total,
                "rate_limit_per_hour": trk.rate_limit or "unknown",
                "estimated_remaining": remaining if remaining is not None else "unknown",
            }
            total += trk.calls_total
        result["total_calls"] = total
        return result

    # ── internals ────────────────────────────────────────────────────

    @staticmethod
    def _parse_headers(trk: _InstanceTracker, response) -> None:
        """Extract rate-limit information from HTTP response headers."""
        headers = getattr(response, "headers", {})

        # ── ServiceNow-style headers ─────────────────────────────────
        # Try both common variations: X-RateLimit and X-Rate-Limit
        limit = headers.get("X-RateLimit-Limit") or headers.get("X-Rate-Limit-Limit")
        if limit is not None:
            try:
                trk.rate_limit = int(limit)
            except (ValueError, TypeError):
                pass

        remaining = headers.get("X-RateLimit-Remaining") or headers.get("X-Rate-Limit-Remaining")
        if remaining is not None and trk.rate_limit > 0:
            try:
                # Synchronise our local counter with the server's truth
                trk.calls_since_reset = trk.rate_limit - int(remaining)
            except (ValueError, TypeError):
                pass

        reset = headers.get("X-RateLimit-Reset") or headers.get("X-Rate-Limit-Reset")
        if reset is not None:
            try:
                new_reset = float(reset)
                # If the window rolled over, restart our counter.
                if new_reset != trk.rate_reset_ts:
                    trk.calls_since_reset = 1
                    trk.rate_reset_ts = new_reset
            except (ValueError, TypeError):
                pass

        retry = headers.get("Retry-After")
        if retry is not None:
            try:
                trk.last_retry_after = int(retry)
            except (ValueError, TypeError):
                pass

        # ── Salesforce-style headers ─────────────────────────────────
        # Format: "api-usage=25/100000"
        sf_info = headers.get("Sforce-Limit-Info")
        if sf_info:
            try:
                usage_part = sf_info.split(",")[0]  # "api-usage=25/100000"
                used, total = usage_part.split("=")[1].split("/")
                trk.rate_limit = int(total)
                trk.calls_since_reset = int(used)
            except (ValueError, IndexError):
                pass
            except (ValueError, IndexError):
                pass
