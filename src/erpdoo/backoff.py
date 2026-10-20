"""Wait for services that are not reachable yet, such as Postgres or Kafka
still starting after a reboot: retry with exponential backoff, logging one
line per retry, until they answer or erpdoo is asked to stop."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from typing import TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")


class Unreachable(Exception):
    """A service did not answer. Raised by checks that retry() should repeat."""


class Stopped(Exception):
    """erpdoo was asked to stop while waiting."""


def reason(error: BaseException) -> str:
    """The first line of an error message (psycopg2's go on with hints)."""
    lines = str(error).strip().splitlines()
    return lines[0].strip() if lines else type(error).__name__


def retry(
    what: str,
    attempt: Callable[[], T],
    errors: tuple[type[BaseException], ...],
    stop: threading.Event,
    first: float = 1.0,
    cap: float = 30.0,
) -> T:
    """Call ``attempt`` until it does not raise one of ``errors``, waiting
    ``first`` seconds after the first failure and twice as long after each
    next one, up to ``cap``. Raises Stopped if ``stop`` is set meanwhile."""
    delay = first
    while True:
        try:
            return attempt()
        except errors as e:
            log.warning("%s is not reachable (%s), retrying in %g s", what, reason(e), delay)
        if stop.wait(delay):
            raise Stopped
        delay = min(delay * 2, cap)
