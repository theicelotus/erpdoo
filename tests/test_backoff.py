import logging
import threading

import pytest

from erpdoo.backoff import Stopped, Unreachable, reason, retry


def test_retries_with_doubling_delay_up_to_the_cap(caplog):
    attempts = []
    waits = []

    class Clock(threading.Event):
        def wait(self, timeout=None):
            waits.append(timeout)
            return False

    def attempt():
        attempts.append(1)
        if len(attempts) < 5:
            raise Unreachable("connection refused")
        return "up"

    with caplog.at_level(logging.WARNING):
        assert retry("Kafka", attempt, (Unreachable,), Clock(), first=1, cap=4) == "up"
    assert waits == [1, 2, 4, 4]
    assert [r.getMessage() for r in caplog.records][:2] == [
        "Kafka is not reachable (connection refused), retrying in 1 s",
        "Kafka is not reachable (connection refused), retrying in 2 s",
    ]


def test_other_errors_are_not_retried():
    def attempt():
        raise ValueError("bad config")

    with pytest.raises(ValueError):
        retry("Postgres", attempt, (Unreachable,), threading.Event())


def test_stop_ends_the_wait():
    stop = threading.Event()

    def attempt():
        stop.set()  # as the SIGTERM handler would, while waiting
        raise Unreachable("down")

    with pytest.raises(Stopped):
        retry("Postgres", attempt, (Unreachable,), stop, first=60)


def test_reason_is_the_first_line():
    error = Exception('connection to server at "postgres" failed: Connection refused\n\tIs the server running?\n')
    assert reason(error) == 'connection to server at "postgres" failed: Connection refused'
    assert reason(TimeoutError()) == "TimeoutError"
