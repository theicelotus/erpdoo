"""Odoo's external JSON-RPC API, as erpdoo's inbound rules use it.

Calls run as one Odoo user, authenticated with an API key (Odoo accepts it
in place of the password). Each call is one Odoo transaction with Odoo's own
access rights, rules and checks.

Failures fall in three kinds, which decide what happens to the message:

- OdooUnavailable: Odoo did not answer (down, restarting, a proxy's 5xx, a
  timeout) or hit a passing database conflict. Retried with backoff.
- OdooSetupError: the API key is refused, or the user lacks an access
  right. Retried too, and logged as an error: an admin has to fix it, and
  the messages should go through once they have.
- OdooError: Odoo refused this message's action (a validation error, a
  user error, a missing record). Not retried; the message is dead-lettered.
"""

from __future__ import annotations

import itertools
import json
import socket
import urllib.error
import urllib.request

# Odoo exceptions that are about the setup, not the message.
SETUP_ERRORS = ("odoo.exceptions.AccessDenied", "odoo.exceptions.AccessError")
# Errors that pass: Odoo retries serialization failures itself a few times
# before giving up on them.
PASSING_ERRORS = ("psycopg2.errors.SerializationFailure", "psycopg2.errors.LockNotAvailable",
                  "psycopg2.errors.DeadlockDetected", "psycopg2.OperationalError",
                  "odoo.exceptions.ConcurrencyError")


class OdooUnavailable(Exception):
    pass


class OdooSetupError(Exception):
    pass


class OdooError(Exception):
    def __init__(self, name: str, message: str) -> None:
        super().__init__(f"{message} ({name})" if name else message)
        self.name = name
        self.message = message


class OdooClient:
    def __init__(self, url: str, db: str, login: str, api_key: str, timeout: float = 60.0) -> None:
        self.url = url.rstrip("/")
        self.db = db
        self.login = login
        self.api_key = api_key
        self.timeout = timeout
        self.uid: int | None = None
        self._ids = itertools.count(1)

    def _rpc(self, service: str, method: str, *args):
        body = json.dumps({"jsonrpc": "2.0", "method": "call", "id": next(self._ids),
                           "params": {"service": service, "method": method, "args": args}}).encode()
        request = urllib.request.Request(f"{self.url}/jsonrpc", body, {"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                reply = json.load(response)
        except urllib.error.HTTPError as e:
            if e.code >= 500 or e.code in (408, 429):
                raise OdooUnavailable(f"Odoo {self.url}: HTTP {e.code}") from None
            raise OdooSetupError(f"Odoo {self.url}: HTTP {e.code} for /jsonrpc") from None
        except (urllib.error.URLError, socket.timeout, ConnectionError, OSError) as e:
            raise OdooUnavailable(f"Odoo {self.url}: {getattr(e, 'reason', e)}") from None
        except ValueError:
            raise OdooUnavailable(f"Odoo {self.url}: not a JSON-RPC answer") from None
        if "error" in reply:
            data = reply["error"].get("data") or {}
            name, message = data.get("name") or "", (data.get("message") or reply["error"].get("message") or "").strip()
            if name in SETUP_ERRORS:
                raise OdooSetupError(f"Odoo refused user {self.login}: {message}")
            if name in PASSING_ERRORS or name.startswith("psycopg2.errors.") and "Serializ" in name:
                raise OdooUnavailable(f"Odoo: {message} ({name})")
            raise OdooError(name, message)
        return reply.get("result")  # Odoo leaves it out when a method returns None

    def authenticate(self) -> int:
        uid = self._rpc("common", "login", self.db, self.login, self.api_key)
        if not uid:
            raise OdooSetupError(f"Odoo refused the API key of user {self.login} on database {self.db}")
        self.uid = uid
        return uid

    def call(self, model: str, method: str, *args, context: dict | None = None, **kwargs):
        if self.uid is None:
            self.authenticate()
        if context:
            kwargs["context"] = context
        return self._rpc("object", "execute_kw", self.db, self.uid, self.api_key, model, method, list(args), kwargs)
