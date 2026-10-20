"""Webhook sink: POSTs each event as JSON, in order, retrying failures.

Network errors, timeouts, HTTP 5xx, 408 and 429 are retried with
exponential backoff. Other 4xx responses mean the receiver rejects the
event, so they fail at once. When an event cannot be delivered, send()
raises and erpdoo stops without confirming it; after a restart the event
is sent again, so a receiver that keeps failing holds up the stream rather
than losing events.
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import logging
import time
import urllib.error
import urllib.request

from erpdoo.events import Event
from erpdoo.sinks import DeliveryError

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {408, 429}
MAX_BACKOFF = 60.0


class WebhookSink:
    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        secret: str | None = None,
        timeout: float = 10.0,
        retries: int = 5,
        backoff: float = 1.0,
        sleep=time.sleep,
    ) -> None:
        self.url = url
        self.headers = headers or {}
        self.secret = secret.encode() if secret else None
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.sleep = sleep

    def send(self, events: list[Event]) -> None:
        for event in events:
            self._post(event)

    def _post(self, event: Event) -> None:
        body = json.dumps(event.payload).encode()
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "erpdoo",
            "X-Erpdoo-Event-Id": event.event_id,
            "X-Erpdoo-Event-Type": event.event_type,
            **self.headers,
        }
        if self.secret:
            # The receiver can recompute this over the raw body to check the sender.
            headers["X-Erpdoo-Signature"] = "sha256=" + hmac.new(self.secret, body, hashlib.sha256).hexdigest()
        delay = self.backoff
        for attempt in range(1, self.retries + 2):
            request = urllib.request.Request(self.url, body, headers, method="POST")
            try:
                with urllib.request.urlopen(request, timeout=self.timeout):
                    return
            except urllib.error.HTTPError as e:
                if e.code < 500 and e.code not in RETRYABLE_STATUS:
                    raise DeliveryError(f"webhook rejected event {event.event_id}: HTTP {e.code}") from e
                error = f"HTTP {e.code}"
                retry_after = e.headers.get("Retry-After", "")
                wait = min(float(retry_after), MAX_BACKOFF) if retry_after.isdigit() else delay
            except (urllib.error.URLError, http.client.HTTPException, OSError) as e:
                error = str(getattr(e, "reason", e))
                wait = delay
            if attempt > self.retries:
                break
            log.warning("webhook: event %s attempt %d failed (%s), retrying in %.1fs",
                        event.event_id, attempt, error, wait)
            self.sleep(wait)
            delay = min(delay * 2, MAX_BACKOFF)
        raise DeliveryError(f"webhook: event {event.event_id} failed after {self.retries + 1} attempts: {error}")

    def close(self) -> None:
        pass
