"""Registering event schemas in a Confluent-compatible schema registry
(Redpanda's, Confluent's, Apicurio's ccompat API).

Each event type has its own subject, named after its schema's record name
(record name strategy): ``<namespace>.<event type>``, e.g.
erpdoo.SaleOrderConfirmed. Several event types share a topic, and each keeps
its own history of versions.

At startup, and when the rules change while running, erpdoo checks every
new schema against the latest registered version of its subject first, and
registers them only if all are compatible; otherwise it stops with the
reasons the registry gives, so a breaking schema is never registered.
erpdoo sets BACKWARD compatibility on each of its subjects that has no
level of its own; a level set by hand is kept.

Messages use the registry's wire format: a zero byte, the schema id (4
bytes, big-endian), then the JSON text or the Avro binary encoding.

Only a few REST calls are needed, so this uses the standard library rather
than confluent-kafka's client, which leaves out the reasons in a failed
compatibility check.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import re
import struct
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from urllib.parse import quote

import fastavro

from erpdoo.backoff import Unreachable
from erpdoo.models import ModelRegistry
from erpdoo.rules import Rule
from erpdoo.schemas import (
    DEFAULT_NAMESPACE, SchemaError, Shape, avro_datum, avro_schema, check_namespace, event_shape, full_name,
    json_schema,
)

log = logging.getLogger(__name__)

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"
LEVEL = "BACKWARD"
SUBJECT_NOT_FOUND, VERSION_NOT_FOUND, SCHEMA_NOT_FOUND, NO_SUBJECT_LEVEL = 40401, 40402, 40403, 40408


class RegistryError(SchemaError):
    def __init__(self, message: str, status: int, code: int | None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


class Client:
    """The REST calls erpdoo makes to the registry."""

    def __init__(self, url: str, username: str | None = None, password: str | None = None,
                 timeout: float = 10.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.headers = {"Content-Type": CONTENT_TYPE, "Accept": CONTENT_TYPE}
        if username is not None:
            token = base64.b64encode(f"{username}:{password or ''}".encode()).decode()
            self.headers["Authorization"] = f"Basic {token}"

    def call(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(f"{self.url}/{path}", data, self.headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as e:
            try:
                detail = json.load(e)
            except ValueError:
                detail = {}
            message = detail.get("message") or e.reason
            if e.code >= 500:
                raise Unreachable(f"schema registry {self.url}: {message} (HTTP {e.code})") from None
            raise RegistryError(f"schema registry {self.url}: {message} (HTTP {e.code})", e.code,
                                detail.get("error_code")) from None
        except (urllib.error.URLError, OSError) as e:
            raise Unreachable(f"schema registry {self.url}: {getattr(e, 'reason', e)}") from None

    def lookup(self, subject: str, schema: dict) -> dict | None:
        """The registered version of exactly this schema, if any."""
        try:
            return self.call("POST", f"subjects/{quote(subject, safe='')}", schema)
        except RegistryError as e:
            if e.code in (SUBJECT_NOT_FOUND, SCHEMA_NOT_FOUND):
                return None
            raise

    def compatibility(self, subject: str) -> str | None:
        """The subject's own compatibility level, if it has one."""
        try:
            return self.call("GET", f"config/{quote(subject, safe='')}")["compatibilityLevel"]
        except RegistryError as e:
            if e.code in (NO_SUBJECT_LEVEL, SUBJECT_NOT_FOUND) or e.status == 404:
                return None
            raise

    def set_compatibility(self, subject: str, level: str) -> None:
        self.call("PUT", f"config/{quote(subject, safe='')}", {"compatibility": level})

    def check(self, subject: str, schema: dict) -> list[str]:
        """Why ``schema`` is not compatible with the subject's latest version
        under the subject's compatibility level; empty if it is (or the
        subject is new)."""
        try:
            reply = self.call("POST", f"compatibility/subjects/{quote(subject, safe='')}/versions/latest?verbose=true",
                              schema)
        except RegistryError as e:
            if e.code in (SUBJECT_NOT_FOUND, VERSION_NOT_FOUND):
                return []
            raise
        if reply.get("is_compatible"):
            return []
        reasons = [r for r in map(describe, reply.get("messages") or []) if r]
        return reasons or ["not compatible (the registry gave no reason)"]

    def register(self, subject: str, schema: dict) -> int:
        return self.call("POST", f"subjects/{quote(subject, safe='')}/versions", schema)["id"]


def describe(message: str) -> str | None:
    """A compatibility message, readable: the description from
    '{errorType:"...", description:"..."}', with JSON Schema paths such as
    '#/properties/record/properties/amount' written as record.amount.
    Context lines (old schema, version, level) give None."""
    if re.match(r"^\{(oldSchemaVersion|oldSchema|compatibility):", message):
        return None
    m = re.search(r"description:\s*(['\"])(.*?)\1\s*(?:,\s*additionalInfo:\s*(['\"])(.*?)\3)?\s*}$", message, re.S)
    text = m.group(2) if m else message
    if m and m.group(4):
        text += f" ({m.group(4)})"

    def path(p: re.Match) -> str:
        parts = [part for part in p.group(1).split("/") if part and part != "properties"]
        return ".".join(parts).replace(".items", "[]") or "the event"

    return re.sub(r"'#(/[^']*)?'", path, text)


@dataclass
class Registered:
    schema_id: int
    shape: Shape
    avro: object | None  # parsed Avro schema


@dataclass
class Plan:
    """The schemas of one sink's rules, checked; apply() registers them."""

    target: Schemas
    subjects: dict[str, tuple[Rule, dict, Shape, object | None]] = field(default_factory=dict)
    known: dict[str, int] = field(default_factory=dict)  # subject -> id, already registered
    problems: list[str] = field(default_factory=list)

    def apply(self) -> None:
        registered = {}
        for subject, (rule, schema, shape, parsed) in self.subjects.items():
            schema_id = self.known.get(subject)
            if schema_id is None:
                try:
                    schema_id = self.target.client.register(subject, schema)
                except RegistryError as e:
                    raise SchemaError(f"{rule.name}: could not register subject {subject}: {e}") from None
                log.info("registered the %s schema of %s as subject %s (id %d)", self.target.format, rule.name,
                         subject, schema_id)
            registered[rule.name] = Registered(schema_id, shape, parsed)
        # Merged, so that the rules still in use keep their schema if
        # another sink's registration fails and the new rules are not used.
        self.target.registered = {**self.target.registered, **registered}


class Schemas:
    """The schemas one Kafka sink writes with, and its serializer."""

    def __init__(self, client: Client, format: str, namespace: str = DEFAULT_NAMESPACE) -> None:
        self.client = client
        self.format = format
        self.namespace = check_namespace(namespace)
        self.registered: dict[str, Registered] = {}
        self._warned: set[str] = set()

    def plan(self, rules: list[Rule], registry: ModelRegistry) -> Plan:
        """Generate each rule's schema and check it against its subject;
        nothing is registered yet."""
        plan = Plan(self)
        for rule in rules:
            subject = full_name(self.namespace, rule)
            shape = event_shape(rule, registry)
            parsed = None
            if self.format == "avro":
                definition = avro_schema(rule, registry, self.namespace)
                parsed = fastavro.parse_schema(definition)
                schema = {"schemaType": "AVRO", "schema": json.dumps(definition)}
            else:
                schema = {"schemaType": "JSON", "schema": json.dumps(json_schema(rule, registry, self.namespace))}
            plan.subjects[subject] = (rule, schema, shape, parsed)
            found = self.client.lookup(subject, schema)
            if found is not None:
                plan.known[subject] = found["id"]
                self._check_level(subject)
                continue
            if self._check_level(subject) is None:
                self.client.set_compatibility(subject, LEVEL)
            reasons = self.client.check(subject, schema)
            if any("different schema type" in r for r in reasons):
                reasons.append(f"the subject holds schemas of another format than {self.format}; a sink in"
                               " another format needs its own namespace (schema_registry: namespace:)")
            plan.problems.extend(f"  {rule.name} (subject {subject}): {reason}" for reason in reasons)
        return plan

    def _check_level(self, subject: str) -> str | None:
        level = self.client.compatibility(subject)
        if level is not None and level != LEVEL and subject not in self._warned:
            self._warned.add(subject)
            log.warning("subject %s has compatibility %s, set by hand: erpdoo registers what that level allows."
                        " Set it back with: curl -X PUT -H 'Content-Type: application/json' -d"
                        " '{\"compatibility\": \"%s\"}' %s/config/%s", subject, level, LEVEL, self.client.url,
                        subject)
        return level

    def serialize(self, event_type: str, payload: dict) -> bytes:
        """The message value of an event, in the registry's wire format.
        Raises SchemaError if the event does not fit its schema (Avro)."""
        entry = self.registered.get(event_type)
        if entry is None:
            raise SchemaError(f"no schema registered for {event_type}")
        header = struct.pack(">bI", 0, entry.schema_id)
        if entry.avro is None:
            return header + json.dumps(payload).encode()
        out = io.BytesIO()
        out.write(header)
        try:
            fastavro.schemaless_writer(out, entry.avro, avro_datum(entry.shape, payload))
        except (ValueError, TypeError) as e:
            raise SchemaError(f"{event_type} does not fit its Avro schema: {e}") from None
        return out.getvalue()


def refused(plans: list[Plan]) -> str:
    """The error message for schemas the registry would refuse."""
    problems = [p for plan in plans for p in plan.problems]
    first = next(plan for plan in plans if plan.problems).target
    subject = problems[0].split("(subject ", 1)[1].split(")", 1)[0]
    return (
        f"the rules changed in a way that breaks the registered event schemas, so erpdoo registered none of"
        f" the new ones (schema registry {first.client.url}, compatibility {LEVEL}: consumers reading with the"
        f" new schema must be able to read every earlier event):\n" + "\n".join(problems) + "\n"
        "Adding fields and includes is compatible; removing a field (JSON Schema) or changing its type is not."
        " Undo the change, or publish it as a new event type (rename the rule) or under a new namespace"
        " (schema_registry: namespace: in erpdoo.yml). To replace the schema anyway, once every consumer is"
        f" ready for it, allow it for the subject, restart erpdoo, then set it back:\n"
        f"  curl -X PUT -H 'Content-Type: application/json' -d '{{\"compatibility\": \"NONE\"}}'"
        f" {first.client.url}/config/{subject}"
    )
