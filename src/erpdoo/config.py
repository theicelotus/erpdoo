"""erpdoo settings file (YAML). Strings may use ${VAR} or ${VAR:-default}
to read environment variables, e.g. for webhook tokens:

    sinks:
      - type: kafka
        bootstrap_servers: ${ERPDOO_KAFKA_BOOTSTRAP:-127.0.0.1:19092}
        topic: erpdoo.events
        format: json              # JSON Schema (default) or avro
        schema_registry:          # optional for json; registers each event type's schema
          url: http://127.0.0.1:18081
      - type: webhook
        url: https://example.com/hooks/erpdoo
        headers: { Authorization: "Bearer ${WEBHOOK_TOKEN}" }
      - type: stdout
    viewer:                       # optional live page of changes and events
      host: 127.0.0.1
      port: 8000

Where changes come from (``source``): Postgres logical replication, the
default, or polling Odoo's JSON-RPC API, for hosted Odoo without database
access (see erpdoo.polling):

    source:
      type: polling
      interval: 5                 # seconds between polls
      odoo: {url: https://example.odoo.com, db: example, login: erpdoo-reader, api_key: ${ERPDOO_POLL_API_KEY}}

How events are delivered (``delivery``): when every sink is Kafka, on one
cluster, in Kafka transactions, exactly once (see erpdoo.sinks.transactions):

    delivery:
      transactions: true          # false: at-least-once, as with other sinks
      transactional_id: erpdoo-1  # default: from the state store, unique per erpdoo
      checkpoint_topic: erpdoo.checkpoints
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import yaml

from erpdoo.health import DEFAULTS, parse_size
from erpdoo.rules import YamlLoader

_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
SETTINGS = {"source", "sinks", "viewer", "slot", "tables", "backfill", "inbound", "delivery"}
INBOUND = {"bootstrap_servers", "group_id", "dead_letter_topic", "odoo", "backoff", "max_backoff", "keep_days"}
ODOO = {"url", "db", "login", "api_key"}
# Polling: seconds between polls; seconds before the cursor read again, for
# transactions that commit after it passed them; seconds between the id
# comparisons that find deleted records (false: never); records per API call.
POLLING = {"interval": 5.0, "overlap": 60.0, "deletes": 300.0, "batch": 500}
DELIVERY = {"transactions": True, "transactional_id": None, "checkpoint_topic": "erpdoo.checkpoints"}


class ConfigError(Exception):
    pass


def _expand(value, env):
    if isinstance(value, str):
        def replace(m):
            name, default = m.group(1), m.group(2)
            if name in env:
                return env[name]
            if default is not None:
                return default
            raise ConfigError(f"environment variable {name} is not set")
        return _VAR.sub(replace, value)
    if isinstance(value, list):
        return [_expand(v, env) for v in value]
    if isinstance(value, dict):
        return {k: _expand(v, env) for k, v in value.items()}
    return value


def load_config(path: str | Path | None, env=os.environ) -> dict:
    """Read the settings file: ``path``, or erpdoo.yml in the working directory
    if it exists. Without one, events go to Kafka as configured by
    ERPDOO_KAFKA_BOOTSTRAP, ERPDOO_TOPIC and ERPDOO_SCHEMA_REGISTRY (optional)."""
    if path is None and Path("erpdoo.yml").exists():
        path = "erpdoo.yml"
    if path is not None and not Path(path).exists():
        raise ConfigError(f"config file {path} not found")
    if path is None:
        kafka = {
            "type": "kafka",
            "bootstrap_servers": env.get("ERPDOO_KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
            "topic": env.get("ERPDOO_TOPIC", "erpdoo.events"),
        }
        if env.get("ERPDOO_SCHEMA_REGISTRY"):
            kafka["schema_registry"] = {"url": env["ERPDOO_SCHEMA_REGISTRY"]}
        return {"source": {"type": "replication"}, "sinks": [kafka], "slot": slot_settings(None), "tables": {},
                "backfill": {"after_lost_slot": True}, "delivery": delivery_settings(None)}
    doc = yaml.load(Path(path).read_text(), Loader=YamlLoader) or {}
    if not isinstance(doc, dict):
        raise ConfigError(f"{path}: expected a mapping")
    unknown = doc.keys() - SETTINGS
    if unknown:
        raise ConfigError(f"{path}: unknown settings {sorted(unknown)}")
    config = _expand(doc, env)
    config["source"] = source_settings(config.get("source"), path)
    sinks = config.get("sinks")
    if not isinstance(sinks, list) or not sinks or not all(isinstance(s, dict) and "type" in s for s in sinks):
        raise ConfigError(f"{path}: 'sinks' must be a non-empty list of entries with a type")
    viewer = config.get("viewer")
    if viewer is not None and (not isinstance(viewer, dict) or viewer.keys() - {"host", "port"}):
        raise ConfigError(f"{path}: 'viewer' takes host and port")
    config["slot"] = slot_settings(config.get("slot"), path)
    tables = config.setdefault("tables", {}) or {}
    if not isinstance(tables, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in tables.items()):
        raise ConfigError(f"{path}: 'tables' maps Odoo model names to table names, e.g. x.gadget: x_gadgets")
    config["tables"] = tables
    backfill = config.get("backfill") or {}
    if not isinstance(backfill, dict) or backfill.keys() - {"after_lost_slot"} or not all(
            isinstance(v, bool) for v in backfill.values()):
        raise ConfigError(f"{path}: 'backfill' takes after_lost_slot: true or false")
    config["backfill"] = {"after_lost_slot": True, **backfill}
    if config.get("inbound") is not None:
        config["inbound"] = inbound_settings(config["inbound"], path)
    config["delivery"] = delivery_settings(config.get("delivery"), path)
    return config


def delivery_settings(spec, path=None) -> dict:
    """The ``delivery`` settings: whether to use Kafka transactions when every
    sink is Kafka (exactly-once, see erpdoo.sinks.transactions), the
    producer's transactional.id (by default from the state store, so unique
    per erpdoo) and the topic of the checkpoints."""
    spec = {} if spec is None else spec
    where = f"{path}: delivery"
    if not isinstance(spec, dict) or spec.keys() - DELIVERY.keys():
        raise ConfigError(f"{where} takes {', '.join(sorted(DELIVERY))}")
    settings = {**DELIVERY, **spec}
    if not isinstance(settings["transactions"], bool):
        raise ConfigError(f"{where}: transactions is true or false")
    for key in ("transactional_id", "checkpoint_topic"):
        if settings[key] is not None and (not isinstance(settings[key], str) or not settings[key]):
            raise ConfigError(f"{where}: {key} is a name")
    return settings


def source_settings(spec, path=None) -> dict:
    """The ``source`` settings: ``replication`` (the default), or polling
    Odoo's API as a read-only Odoo user, with the POLLING defaults."""
    where = f"{path}: source"
    if spec is None or spec == "replication" or spec == {"type": "replication"}:
        return {"type": "replication"}
    if spec == "polling":
        raise ConfigError(f"{where}: polling needs the Odoo to poll: type: polling and odoo: {{url, db, login,"
                          " api_key}}")
    if not isinstance(spec, dict) or spec.get("type") not in ("replication", "polling"):
        raise ConfigError(f"{where} is replication, or a mapping with type: polling")
    if spec["type"] == "replication":
        raise ConfigError(f"{where}: replication takes no settings (Postgres is set with ERPDOO_PG_DSN)")
    unknown = spec.keys() - POLLING.keys() - {"type", "odoo"}
    if unknown:
        raise ConfigError(f"{where}: unknown settings {sorted(unknown)}; polling takes"
                          f" {', '.join(sorted(POLLING))} and odoo")
    odoo = spec.get("odoo")
    if not isinstance(odoo, dict) or odoo.keys() != ODOO or not all(odoo.values()):
        raise ConfigError(f"{where}: odoo needs {', '.join(sorted(ODOO))} (an API key of a read-only Odoo user,"
                          " see docs/polling.md)")
    settings = {**POLLING, **{k: v for k, v in spec.items() if k in POLLING}}
    if settings["deletes"] is False:
        settings["deletes"] = None
    try:
        if any(isinstance(v, bool) for v in settings.values()):
            raise ValueError
        numbers = {k: float(v) for k, v in settings.items() if v is not None}
    except (TypeError, ValueError):
        raise ConfigError(f"{where}: {', '.join(sorted(POLLING))} are numbers (deletes may be false)") from None
    if numbers["interval"] <= 0 or numbers["overlap"] < 0 or numbers["batch"] < 1 or numbers.get("deletes", 1) <= 0:
        raise ConfigError(f"{where}: interval, deletes and batch must be positive, overlap not negative")
    return {**settings, **numbers, "batch": int(numbers["batch"]), "type": "polling",
            "odoo": {k: str(v) for k, v in odoo.items()}}


def inbound_settings(spec, path=None) -> dict:
    """The ``inbound`` settings: where messages come from, and the Odoo user
    the inbound rules act as."""
    where = f"{path}: inbound"
    if not isinstance(spec, dict) or spec.keys() - INBOUND or not {"bootstrap_servers", "odoo"} <= spec.keys():
        raise ConfigError(f"{where} takes {', '.join(sorted(INBOUND))} (bootstrap_servers and odoo are required)")
    odoo = spec["odoo"]
    if not isinstance(odoo, dict) or odoo.keys() != ODOO or not all(odoo.values()):
        raise ConfigError(f"{where}: odoo needs {', '.join(sorted(ODOO))} (an API key of the"
                          " integration user, see docs/inbound.md)")
    try:
        numbers = {k: float(spec[k]) for k in ("backoff", "max_backoff", "keep_days") if k in spec}
    except (TypeError, ValueError):
        raise ConfigError(f"{where}: backoff, max_backoff and keep_days are numbers") from None
    return {**spec, **numbers, "odoo": {k: str(v) for k, v in odoo.items()}}


def slot_settings(spec, path=None) -> dict:
    """The ``slot`` settings with defaults: warn when the slot is more than
    ``lag_warning`` bytes (or "1GB") or ``lag_warning_seconds`` behind, and
    report its lag every ``report_seconds``."""
    spec = spec or {}
    if not isinstance(spec, dict) or spec.keys() - DEFAULTS.keys():
        raise ConfigError(f"{path}: 'slot' takes {', '.join(sorted(DEFAULTS))}")
    try:
        return {**DEFAULTS, **{k: parse_size(v) if k == "lag_warning" else float(v) for k, v in spec.items()}}
    except ValueError as e:
        raise ConfigError(f"{path}: slot: {e}") from None
