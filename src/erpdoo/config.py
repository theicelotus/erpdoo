"""erpdoo settings file (YAML). Strings may use ${VAR} or ${VAR:-default}
to read environment variables, e.g. for webhook tokens:

    sinks:
      - type: kafka
        bootstrap_servers: ${ERPDOO_KAFKA_BOOTSTRAP:-127.0.0.1:19092}
        topic: erpdoo.events
      - type: webhook
        url: https://example.com/hooks/erpdoo
        headers: { Authorization: "Bearer ${WEBHOOK_TOKEN}" }
      - type: stdout
    viewer:                       # optional live page of changes and events
      host: 127.0.0.1
      port: 8000
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import yaml

from erpdoo.rules import YamlLoader

_VAR = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
SETTINGS = {"sinks", "viewer"}


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
    ERPDOO_KAFKA_BOOTSTRAP and ERPDOO_TOPIC."""
    if path is None and Path("erpdoo.yml").exists():
        path = "erpdoo.yml"
    if path is not None and not Path(path).exists():
        raise ConfigError(f"config file {path} not found")
    if path is None:
        return {"sinks": [{
            "type": "kafka",
            "bootstrap_servers": env.get("ERPDOO_KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
            "topic": env.get("ERPDOO_TOPIC", "erpdoo.events"),
        }]}
    doc = yaml.load(Path(path).read_text(), Loader=YamlLoader) or {}
    if not isinstance(doc, dict):
        raise ConfigError(f"{path}: expected a mapping")
    unknown = doc.keys() - SETTINGS
    if unknown:
        raise ConfigError(f"{path}: unknown settings {sorted(unknown)}")
    config = _expand(doc, env)
    sinks = config.get("sinks")
    if not isinstance(sinks, list) or not sinks or not all(isinstance(s, dict) and "type" in s for s in sinks):
        raise ConfigError(f"{path}: 'sinks' must be a non-empty list of entries with a type")
    viewer = config.get("viewer")
    if viewer is not None and (not isinstance(viewer, dict) or viewer.keys() - {"host", "port"}):
        raise ConfigError(f"{path}: 'viewer' takes host and port")
    return config
