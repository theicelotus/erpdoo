"""Event rules, written in YAML:

    SaleOrderConfirmed:
      model: sale.order
      when:
        state: { to: sale }                # value changed to sale
      fields: [name, amount_total]         # optional, default: every stored column
      include:
        partner_id: [name, email]          # many2one, with the fields to keep
        order_line: [name, price_subtotal] # one2many
        order_line.product_id: [name]      # nested; related fields like name work
        invoice_line_ids:                  # with a filter on the related rows
          fields: [name]
          where: { display_type: product }

Conditions under ``when``:
    field: value or [values]        value at commit is one of these
    field: { to: v }                changed to v (v may be a list)
    field: { from: v, to: w }       changed from v to w
    field: { changed: true }        changed at all
``on`` limits the trigger to [create, update, delete]; the default is create
and update. Everything is compared per transaction: the row at its start
against the row at its commit.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from erpdoo.models import Field, ModelRegistry

TRIGGERS = frozenset({"create", "update", "delete"})
RULE_KEYS = {"model", "when", "on", "fields", "include"}


class RuleError(Exception):
    pass


class YamlLoader(yaml.SafeLoader):
    """YAML 1.2 booleans only: PyYAML follows YAML 1.1, where on, off, yes and
    no are booleans. That would turn the key "on" into True and an Odoo
    selection value such as invoice_status "no" into False."""


YamlLoader.yaml_implicit_resolvers = {
    first: [(tag, regexp) for tag, regexp in resolvers if tag != "tag:yaml.org,2002:bool"]
    for first, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
YamlLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool", re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"), list("tTfF")
)


def _values(v) -> frozenset:
    return frozenset(v) if isinstance(v, list) else frozenset([v])


@dataclass(frozen=True)
class Condition:
    field: str
    equals: frozenset | None = None
    from_: frozenset | None = None
    to: frozenset | None = None
    changed: bool = False

    @property
    def is_transition(self) -> bool:
        return self.changed or self.from_ is not None or self.to is not None

    def matches(self, before: dict, after: dict) -> bool:
        old, new = before.get(self.field), after.get(self.field)
        if self.equals is not None and new not in self.equals:
            return False
        if self.is_transition and old == new:
            return False
        if self.from_ is not None and old not in self.from_:
            return False
        return self.to is None or new in self.to

    @classmethod
    def parse(cls, name: str, spec) -> Condition:
        if not isinstance(spec, dict):
            return cls(name, equals=_values(spec))
        unknown = spec.keys() - {"from", "to", "changed"}
        if unknown:
            raise RuleError(f"condition on {name}: unknown keys {sorted(unknown)}")
        return cls(
            name,
            from_=_values(spec["from"]) if "from" in spec else None,
            to=_values(spec["to"]) if "to" in spec else None,
            changed=bool(spec.get("changed", False)),
        )


@dataclass
class Include:
    """A relational field to expand, with what to keep of the related rows."""

    field: str
    fields: tuple[str, ...] | None = None  # None: every stored column
    where: tuple[Condition, ...] = ()
    children: dict[str, Include] = field(default_factory=dict)


@dataclass(frozen=True)
class Rule:
    name: str
    model: str
    on: frozenset[str]
    conditions: tuple[Condition, ...]
    fields: tuple[str, ...] | None
    includes: dict[str, Include]

    def matches(self, before: dict | None, after: dict | None) -> bool:
        """before is {} for a record created in the transaction, after is None
        for a deleted one."""
        op = "delete" if after is None else "create" if before == {} else "update"
        return op in self.on and all(c.matches(before or {}, after or {}) for c in self.conditions)


def load_rules(path: str | Path) -> list[Rule]:
    """Read rules from a YAML file, or from every .yml/.yaml file in a directory."""
    path = Path(path)
    files = sorted(p for p in path.iterdir() if p.suffix in (".yml", ".yaml")) if path.is_dir() else [path]
    rules: dict[str, Rule] = {}
    for file in files:
        doc = yaml.load(file.read_text(), Loader=YamlLoader) or {}
        if not isinstance(doc, dict):
            raise RuleError(f"{file}: expected a mapping of event names to rules")
        for name, spec in doc.items():
            if name in rules:
                raise RuleError(f"{file}: rule {name} is defined twice")
            try:
                rules[name] = parse_rule(name, spec)
            except RuleError as e:
                raise RuleError(f"{file}: {e}") from None
    return list(rules.values())


def parse_rule(name: str, spec) -> Rule:
    if not isinstance(spec, dict) or "model" not in spec:
        raise RuleError(f"rule {name}: needs at least a model")
    unknown = spec.keys() - RULE_KEYS
    if unknown:
        raise RuleError(f"rule {name}: unknown keys {sorted(unknown)}")
    on = _values(spec.get("on", ["create", "update"]))
    if not on <= TRIGGERS:
        raise RuleError(f"rule {name}: 'on' must be among {sorted(TRIGGERS)}")
    try:
        conditions = tuple(Condition.parse(f, c) for f, c in (spec.get("when") or {}).items())
    except RuleError as e:
        raise RuleError(f"rule {name}: {e}") from None
    fields = tuple(spec["fields"]) if spec.get("fields") is not None else None
    return Rule(name, spec["model"], on, conditions, fields, _parse_includes(name, spec.get("include") or {}))


def _parse_includes(rule: str, spec) -> dict[str, Include]:
    if isinstance(spec, list):
        spec = {path: None for path in spec}
    roots: dict[str, Include] = {}
    # Parents first, so "order_line" settings apply before "order_line.product_id".
    for path in sorted(spec, key=lambda p: p.count(".")):
        options = spec[path]
        if options is None or options == "all":
            fields, where = None, ()
        elif isinstance(options, list):
            fields, where = tuple(options), ()
        elif isinstance(options, dict):
            unknown = options.keys() - {"fields", "where"}
            if unknown:
                raise RuleError(f"rule {rule}: include {path}: unknown keys {sorted(unknown)}")
            fields = tuple(options["fields"]) if options.get("fields") is not None else None
            where = tuple(Condition(f, equals=_values(v)) for f, v in (options.get("where") or {}).items())
        else:
            raise RuleError(f"rule {rule}: include {path}: expected a list of fields or a mapping")
        *parents, leaf = path.split(".")
        level = roots
        for part in parents:
            level = level.setdefault(part, Include(part)).children
        node = level.setdefault(leaf, Include(leaf))
        node.fields, node.where = fields, where
    return roots


def related_hops(registry: ModelRegistry, model: str, name: str, depth: int = 0) -> list[tuple[str, Field]]:
    """How to read field ``name`` of ``model``: the many2one fields to follow,
    then the field that holds the value, each with the model it belongs to.
    Raises RuleError if the value is not stored anywhere reachable."""
    f = registry.models[model].fields.get(name)
    if f is None:
        raise RuleError(f"{model} has no field {name}")
    if f.column:
        return [(model, f)]
    if f.related and depth < 5:
        hops = []
        *steps, last = f.related.split(".")
        for step in steps:
            hop = registry.models[model].fields.get(step)
            if hop is None or hop.type != "many2one" or not hop.column or hop.relation not in registry.models:
                break
            hops.append((model, hop))
            model = hop.relation
        else:
            return hops + related_hops(registry, model, last, depth + 1)
    raise RuleError(f"{model}.{name} is not stored and is not a related field of stored many2one fields")


def validate(rules: list[Rule], registry: ModelRegistry) -> None:
    """Check rules against the Odoo models; raise one RuleError listing every problem."""
    problems = []
    for rule in rules:
        try:
            _validate(rule, registry)
        except RuleError as e:
            problems.append(f"rule {rule.name}: {e}")
    if problems:
        raise RuleError("\n".join(problems))


def _validate(rule: Rule, registry: ModelRegistry) -> None:
    if rule.model not in registry.models:
        raise RuleError(f"unknown model {rule.model} (or it has no table)")
    model = registry.models[rule.model]
    for c in rule.conditions:
        if c.field not in model.fields or not model.fields[c.field].column:
            raise RuleError(f"condition field {c.field} is not a stored column of {rule.model}")
    for name in rule.fields or ():
        related_hops(registry, rule.model, name)
    _validate_includes(registry, rule.model, rule.includes)


def _validate_includes(registry: ModelRegistry, model: str, includes: dict[str, Include]) -> None:
    for inc in includes.values():
        target = _include_target(registry, model, inc.field)
        for name in inc.fields or ():
            related_hops(registry, target, name)
        for c in inc.where:
            f = registry.models[target].fields.get(c.field)
            if f is None or not f.column:
                raise RuleError(f"include {inc.field}: where field {c.field} is not a stored column of {target}")
        _validate_includes(registry, target, inc.children)


def _include_target(registry: ModelRegistry, model: str, name: str) -> str:
    f = registry.models[model].fields.get(name)
    if f is None:
        raise RuleError(f"{model} has no field {name}")
    if f.type == "many2many":
        raise RuleError(f"include {model}.{name}: many2many includes are not supported yet")
    if f.type not in ("many2one", "one2many") or f.relation not in registry.models:
        raise RuleError(f"include {model}.{name}: not a many2one or one2many field to a model with a table")
    if f.type == "many2one" and not f.column:
        raise RuleError(f"include {model}.{name}: many2one is not stored")
    if f.type == "one2many":
        inverse = registry.models[f.relation].fields.get(f.inverse or "")
        if inverse is None or not inverse.column:
            raise RuleError(f"include {model}.{name}: inverse field {f.relation}.{f.inverse} is not a stored column")
    return f.relation


def tracked_tables(rules: list[Rule], registry: ModelRegistry) -> dict[str, set[str]]:
    """Tables the state store must copy for these rules, each with the columns
    to index for one2many lookups."""
    tables: dict[str, set[str]] = {}

    def need_fields(model: str, names) -> None:
        for name in names or ():
            for _model, hop in related_hops(registry, model, name)[:-1]:
                tables.setdefault(registry.models[hop.relation].table, set())

    def walk(model: str, includes: dict[str, Include]) -> None:
        for inc in includes.values():
            f = registry.models[model].fields[inc.field]
            target = registry.models[f.relation]
            columns = tables.setdefault(target.table, set())
            if f.type == "one2many":
                columns.add(registry.models[f.relation].fields[f.inverse].column)
            need_fields(target.name, inc.fields)
            walk(target.name, inc.children)

    for rule in rules:
        need_fields(rule.model, rule.fields)
        walk(rule.model, rule.includes)
    return tables
