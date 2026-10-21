"""Inbound rules: messages from other systems that act on Odoo, written in
YAML (rules/inbound/ by default, ERPDOO_INBOUND_RULES):

    DeliveryShipped:
      topic: warehouse.events            # Kafka topic the messages arrive on
      match: { type: DeliveryShipped }   # optional; default: field "type" is the rule's name
      id: id                             # optional; the message's unique id (a dotted path)
      model: stock.picking
      find:                              # the one record to act on
        name: "{reference}"              # message fields in braces; a list value means "in"
        picking_type_code: outgoing
      already_done: { state: done }      # optional, see below
      steps:
        - write: { carrier_tracking_ref: "{tracking_number}" }
        - call: button_validate          # a public method of the record
          context: { skip_backorder: true }
      expect: { state: done }            # optional; checked after the steps

A step is one of:
    write: {field: value}               write fields on the record
    call: method                        call a method on the record (args: [...] optional)
    wizard: model                       create a wizard for the record (context active_model,
      values: {field: value}            active_id, active_ids) and call its method
      call: method

"{path}" alone keeps the value's type (a number stays a number); inside a
longer string it is formatted into it. "{record.id}" is the record found.

already_done says when the message's action is done already, for instance
because erpdoo stopped after Odoo carried it out but before it could note
the message as processed: either filters the record found must match
(``{state: done}``), or another search (``{model: account.payment, find:
{memo: "{payment_id}"}}``). The message is then noted as processed without
calling Odoo again.

Each step is one Odoo call, in its own transaction; a message counts as
processed only once every step succeeded. A retried message runs every
step again, so steps should be safe to repeat (writes are).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from erpdoo.models import ModelRegistry
from erpdoo.odoo_rpc import OdooClient
from erpdoo.rules import RuleError, YamlLoader

RULE_KEYS = {"topic", "match", "id", "model", "find", "already_done", "steps", "expect"}
_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_.]*)\}")


class MessageError(Exception):
    """The message cannot be applied, now or later (permanent)."""


@dataclass(frozen=True)
class Step:
    kind: str  # write, call or wizard
    values: dict = field(default_factory=dict)  # write, wizard
    method: str | None = None  # call, wizard
    args: tuple = ()
    context: dict = field(default_factory=dict)
    model: str | None = None  # wizard

    def describe(self) -> str:
        if self.kind == "write":
            return f"write {', '.join(self.values)}"
        if self.kind == "call":
            return f"call {self.method}"
        return f"wizard {self.model}.{self.method}"


@dataclass(frozen=True)
class InboundRule:
    name: str
    topic: str
    match: dict
    id_path: str
    model: str
    find: dict
    steps: tuple[Step, ...]
    already_done: dict | None = None  # {"model": None or a model, "find": {...}}
    expect: dict = field(default_factory=dict)

    def matches(self, message: dict) -> bool:
        return all(lookup(message, path, default=_MISSING) == value for path, value in self.match.items())


@dataclass
class Outcome:
    status: str  # done, already_done
    record_id: int
    detail: str = ""


_MISSING = object()


def lookup(message: dict, path: str, default=None):
    value = message
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            if default is not None:
                return default
            raise MessageError(f"the message has no {path}")
        value = value[part]
    return value


def render(template, message: dict, record_id: int | None = None):
    """A template with the message's values (see the module docstring)."""
    if isinstance(template, dict):
        return {k: render(v, message, record_id) for k, v in template.items()}
    if isinstance(template, list):
        return [render(v, message, record_id) for v in template]
    if not isinstance(template, str):
        return template

    def value(path: str):
        if path == "record.id":
            if record_id is None:
                raise MessageError("{record.id} is not known yet here")
            return record_id
        return lookup(message, path)

    whole = _PLACEHOLDER.fullmatch(template)
    if whole:
        return value(whole.group(1))
    return _PLACEHOLDER.sub(lambda m: str(value(m.group(1))), template)


def domain(filters: dict) -> list:
    return [(name, "in" if isinstance(v, list) else "=", v) for name, v in filters.items()]


def load_inbound_rules(path: str | Path) -> list[InboundRule]:
    """Rules from a YAML file, or every .yml/.yaml file in a directory. A
    directory that does not exist gives none."""
    path = Path(path)
    if not path.exists():
        return []
    files = sorted(p for p in path.iterdir() if p.suffix in (".yml", ".yaml")) if path.is_dir() else [path]
    rules: dict[str, InboundRule] = {}
    for file in files:
        doc = yaml.load(file.read_text(), Loader=YamlLoader) or {}
        if not isinstance(doc, dict):
            raise RuleError(f"{file}: expected a mapping of message types to inbound rules")
        for name, spec in doc.items():
            if name in rules:
                raise RuleError(f"{file}: inbound rule {name} is defined twice")
            try:
                rules[name] = parse_inbound_rule(name, spec)
            except RuleError as e:
                raise RuleError(f"{file}: {e}") from None
    return list(rules.values())


def parse_inbound_rule(name: str, spec) -> InboundRule:
    where = f"inbound rule {name}"
    if not isinstance(spec, dict) or not {"topic", "model", "find", "steps"} <= spec.keys():
        raise RuleError(f"{where}: needs topic, model, find and steps")
    unknown = spec.keys() - RULE_KEYS
    if unknown:
        raise RuleError(f"{where}: unknown keys {sorted(unknown)}")
    if not isinstance(spec["find"], dict) or not spec["find"]:
        raise RuleError(f"{where}: find must map fields to values")
    if not isinstance(spec["steps"], list) or not spec["steps"]:
        raise RuleError(f"{where}: steps must be a non-empty list")
    steps = tuple(_parse_step(where, s) for s in spec["steps"])
    done = spec.get("already_done")
    if done is not None:
        if not isinstance(done, dict) or not done:
            raise RuleError(f"{where}: already_done must be filters, or a model and find")
        done = ({"model": done["model"], "find": done.get("find") or {}} if "model" in done
                else {"model": None, "find": done})
    return InboundRule(name, str(spec["topic"]), dict(spec.get("match") or {"type": name}), str(spec.get("id", "id")),
                       spec["model"], dict(spec["find"]), steps, done, dict(spec.get("expect") or {}))


def _parse_step(where: str, spec) -> Step:
    if not isinstance(spec, dict):
        raise RuleError(f"{where}: a step is a mapping with write, call or wizard")
    kinds = spec.keys() & {"write", "call", "wizard"}
    if "wizard" in kinds:
        kinds = {"wizard"}
    if len(kinds) != 1:
        raise RuleError(f"{where}: each step has one of write, call or wizard")
    kind = kinds.pop()
    allowed = {"write": {"write", "context"}, "call": {"call", "args", "context"},
               "wizard": {"wizard", "values", "call", "context"}}[kind]
    unknown = spec.keys() - allowed
    if unknown:
        raise RuleError(f"{where}: {kind} step: unknown keys {sorted(unknown)}")
    context = dict(spec.get("context") or {})
    if kind == "write":
        if not isinstance(spec["write"], dict) or not spec["write"]:
            raise RuleError(f"{where}: write takes fields and values")
        return Step("write", values=dict(spec["write"]), context=context)
    method = spec.get("call")
    if not isinstance(method, str) or not method or method.startswith("_"):
        raise RuleError(f"{where}: {kind} step needs call: a public method name (Odoo refuses names starting with _)")
    if kind == "call":
        return Step("call", method=method, args=tuple(spec.get("args") or ()), context=context)
    return Step("wizard", values=dict(spec.get("values") or {}), method=method, context=context, model=spec["wizard"])


def validate_inbound(rules: list[InboundRule], registry: ModelRegistry) -> tuple[list[InboundRule], list[str]]:
    """The rules whose model exists, and why the others are skipped. Raises
    RuleError listing every field or model in an active rule Odoo does not have."""
    active, skipped, problems = [], [], []
    for rule in rules:
        if rule.model not in registry.models:
            skipped.append(f"inbound rule {rule.name} skipped: model {rule.model} not found (is its Odoo app installed?)")
            continue
        active.append(rule)

        def check(model: str, names, what: str) -> None:
            if model not in registry.models:
                problems.append(f"inbound rule {rule.name}: {what}: unknown model {model}")
                return
            for name in names:
                if name.split(".")[0] not in registry.models[model].fields:
                    problems.append(f"inbound rule {rule.name}: {what}: {model} has no field {name}")

        check(rule.model, rule.find, "find")
        check(rule.model, rule.expect, "expect")
        if rule.already_done:
            check(rule.already_done["model"] or rule.model, rule.already_done["find"], "already_done")
        for step in rule.steps:
            if step.kind == "write":
                check(rule.model, step.values, "write")
            elif step.kind == "wizard":
                check(step.model, step.values, "wizard")
    if problems:
        raise RuleError("\n".join(problems))
    return active, skipped


def apply(rule: InboundRule, message: dict, odoo: OdooClient) -> Outcome:
    """Carry out a message's action in Odoo. Raises MessageError or
    odoo_rpc.OdooError if it cannot be done, and OdooUnavailable or
    OdooSetupError if it should be tried again later."""
    filters = render(rule.find, message)
    ids = odoo.call(rule.model, "search", domain(filters), limit=2)
    what = ", ".join(f"{k} = {v!r}" for k, v in filters.items())
    if not ids:
        raise MessageError(f"no {rule.model} with {what}")
    if len(ids) > 1:
        raise MessageError(f"more than one {rule.model} with {what}")
    record_id = ids[0]
    if rule.already_done:
        done = render(rule.already_done["find"], message, record_id)
        model = rule.already_done["model"] or rule.model
        scope = [] if rule.already_done["model"] else [("id", "=", record_id)]
        if odoo.call(model, "search", scope + domain(done), limit=1):
            return Outcome("already_done", record_id, f"{model} with {done} exists")
    results = []
    for step in rule.steps:
        context = render(step.context, message, record_id)
        if step.kind == "write":
            odoo.call(rule.model, "write", [record_id], render(step.values, message, record_id), context=context)
            results.append(None)
        elif step.kind == "call":
            args = render(list(step.args), message, record_id)
            results.append(odoo.call(rule.model, step.method, [record_id], *args, context=context))
        else:
            context = {"active_model": rule.model, "active_id": record_id, "active_ids": [record_id], **context}
            wizard = odoo.call(step.model, "create", render(step.values, message, record_id), context=context)
            results.append(odoo.call(step.model, step.method, [wizard], context=context))
    if rule.expect:
        [row] = odoo.call(rule.model, "read", [record_id], list(rule.expect)) or [{}]
        wrong = {name: row.get(name) for name, want in rule.expect.items() if not _fits(row.get(name), want)}
        if wrong:
            # A method that needs a person's answer returns a wizard action instead of acting.
            asked = [r.get("res_model") or r.get("name") for r in results if isinstance(r, dict) and r.get("type")]
            raise MessageError(f"after {', '.join(s.describe() for s in rule.steps)}, {rule.model} {record_id} has"
                               f" {wrong}, expected {rule.expect}"
                               + (f" (Odoo asked for {', '.join(map(str, asked))})" if asked else ""))
    return Outcome("done", record_id)


def _fits(value, want) -> bool:
    if isinstance(value, list) and len(value) == 2 and isinstance(value[0], int):
        value = value[0]  # many2one: [id, name]
    if value is False:
        value = None
    return value in want if isinstance(want, list) else value == want
