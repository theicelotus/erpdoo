"""Replication slot health: how far erpdoo's slot trails Postgres' WAL.

Postgres keeps every WAL file the slot has not confirmed, so a slot that
falls behind (erpdoo stopped, or a sink blocking it) fills the disk, or,
with max_slot_wal_keep_size set, gets invalidated. erpdoo logs the lag
every minute and warns when it passes a threshold.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass

_SIZE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]?)i?b?\s*$", re.IGNORECASE)
_UNITS = {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}

DEFAULTS = {"lag_warning": 1024**3, "lag_warning_seconds": 300, "report_seconds": 60}


def parse_size(value) -> int:
    """Bytes from 1073741824, "1GB", "512 MB" or "64kB" (1024-based, as Postgres)."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    m = _SIZE.match(str(value))
    if not m:
        raise ValueError(f"not a size: {value!r} (expected e.g. 1GB or 512MB)")
    return int(float(m.group(1)) * _UNITS[m.group(2).lower()])


def format_size(n: int) -> str:
    for unit in ("B", "kB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


@dataclass(frozen=True)
class SlotHealth:
    slot: str
    lag_bytes: int  # WAL the slot holds that erpdoo has not confirmed
    # How long WAL waits before erpdoo confirms it, as Postgres measures it
    # (pg_stat_replication.flush_lag); None until Postgres has measured it
    # for this connection, or when there was nothing to confirm for a while.
    lag_seconds: float | None
    wal_status: str  # reserved, extended, unreserved (about to be invalidated) or lost
    safe_wal_size: int | None  # WAL left before max_slot_wal_keep_size invalidates the slot

    def describe(self) -> str:
        behind = "time behind not measured" if self.lag_seconds is None else f"{self.lag_seconds:.1f} s behind"
        text = f"slot {self.slot}: {format_size(self.lag_bytes)} of WAL not confirmed yet, {behind}"
        if self.safe_wal_size is not None:
            text += f", invalidated after {format_size(max(self.safe_wal_size, 0))} more"
        return text

    def warnings(self, settings: dict) -> list[str]:
        """Why this needs attention, given the configured thresholds."""
        found = []
        if self.lag_bytes > settings["lag_warning"]:
            found.append(f"more than {format_size(settings['lag_warning'])} of WAL behind")
        if self.lag_seconds is not None and self.lag_seconds > settings["lag_warning_seconds"]:
            found.append(f"more than {settings['lag_warning_seconds']:g} s behind")
        if self.wal_status == "unreserved":
            found.append("Postgres is about to invalidate the slot (max_slot_wal_keep_size)")
        elif self.wal_status == "lost":
            found.append("Postgres has invalidated the slot (max_slot_wal_keep_size)")
        return found

    def as_dict(self, settings: dict) -> dict:
        return {**asdict(self), "warnings": self.warnings(settings), "text": self.describe()}


def query(cur, slot: str) -> SlotHealth | None:
    """The slot's health, or None if it does not exist."""
    cur.execute(
        "SELECT pg_current_wal_lsn() - s.confirmed_flush_lsn, extract(epoch FROM r.flush_lag),"
        " s.wal_status, s.safe_wal_size"
        " FROM pg_replication_slots s LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid"
        " WHERE s.slot_name = %s",
        [slot],
    )
    row = cur.fetchone()
    if row is None:
        return None
    lag, seconds, status, safe = row
    return SlotHealth(slot, int(lag or 0), float(seconds) if seconds is not None else None, status or "lost",
                      int(safe) if safe is not None else None)
