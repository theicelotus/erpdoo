import pytest

from erpdoo.config import ConfigError, load_config


def write(tmp_path, text):
    path = tmp_path / "erpdoo.yml"
    path.write_text(text)
    return path


def test_env_expansion(tmp_path):
    path = write(tmp_path, """
sinks:
  - type: webhook
    url: ${HOOK_URL}
    headers: { Authorization: "Bearer ${TOKEN}" }
    secret: ${SECRET:-}
  - type: kafka
    bootstrap_servers: ${BROKERS:-localhost:9092}
    topic: events
""")
    config = load_config(path, env={"HOOK_URL": "https://x/hook", "TOKEN": "abc"})
    assert config["sinks"][0] == {"type": "webhook", "url": "https://x/hook",
                                  "headers": {"Authorization": "Bearer abc"}, "secret": ""}
    assert config["sinks"][1]["bootstrap_servers"] == "localhost:9092"


def test_missing_variable_is_an_error(tmp_path):
    with pytest.raises(ConfigError, match="TOKEN is not set"):
        load_config(write(tmp_path, "sinks: [{type: webhook, url: '${TOKEN}'}]"), env={})


def test_validation(tmp_path):
    with pytest.raises(ConfigError, match="unknown settings"):
        load_config(write(tmp_path, "sink: []"), env={})
    with pytest.raises(ConfigError, match="non-empty list"):
        load_config(write(tmp_path, "sinks: []"), env={})
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "missing.yml", env={})


def test_default_without_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config = load_config(None, env={"ERPDOO_TOPIC": "t"})
    assert config["sinks"] == [{"type": "kafka", "bootstrap_servers": "127.0.0.1:19092", "topic": "t"}]


def test_tables_setting(tmp_path):
    path = write(tmp_path, "sinks: [{type: stdout}]\ntables: {x.gadget: x_custom_gadgets}\n")
    assert load_config(path)["tables"] == {"x.gadget": "x_custom_gadgets"}
    assert load_config(write(tmp_path, "sinks: [{type: stdout}]\n"))["tables"] == {}
    with pytest.raises(ConfigError, match="maps Odoo model names"):
        load_config(write(tmp_path, "sinks: [{type: stdout}]\ntables: [x]\n"))


def test_backfill_setting(tmp_path):
    assert load_config(write(tmp_path, "sinks: [{type: stdout}]\n"))["backfill"] == {"after_lost_slot": True}
    path = write(tmp_path, "sinks: [{type: stdout}]\nbackfill: {after_lost_slot: false}\n")
    assert load_config(path)["backfill"] == {"after_lost_slot": False}
    with pytest.raises(ConfigError, match="after_lost_slot"):
        load_config(write(tmp_path, "sinks: [{type: stdout}]\nbackfill: {after_lost_slot: maybe}\n"))


def test_inbound_settings(tmp_path):
    path = write(tmp_path, """
sinks: [{type: stdout}]
inbound:
  bootstrap_servers: localhost:9092
  backoff: 2
  odoo: {url: "http://odoo:8069", db: odoo, login: erpdoo-integration, api_key: "${KEY}"}
""")
    inbound = load_config(path, env={"KEY": "k"})["inbound"]
    assert inbound["backoff"] == 2.0 and inbound["odoo"]["api_key"] == "k"
    for text, error in [
        ("inbound: {odoo: {}}", "bootstrap_servers and odoo are required"),
        ("inbound: {bootstrap_servers: x, odoo: {url: u, db: d, login: l}}", "odoo needs api_key, db, login, url"),
        ("inbound: {bootstrap_servers: x, odoo: {url: u, db: d, login: l, api_key: k}, keep_days: soon}",
         "are numbers"),
    ]:
        with pytest.raises(ConfigError, match=error):
            load_config(write(tmp_path, "sinks: [{type: stdout}]\n" + text))


def test_source_setting(tmp_path):
    for text in ("", "source: replication\n", "source: {type: replication}\n"):
        assert load_config(write(tmp_path, "sinks: [{type: stdout}]\n" + text))["source"] == {"type": "replication"}
    path = write(tmp_path, """
sinks: [{type: stdout}]
source:
  type: polling
  interval: 2
  deletes: false
  odoo: {url: "https://example.odoo.com", db: example, login: erpdoo-reader, api_key: "${KEY}"}
""")
    assert load_config(path, env={"KEY": "k"})["source"] == {
        "type": "polling", "interval": 2.0, "overlap": 60.0, "deletes": None, "batch": 500,
        "odoo": {"url": "https://example.odoo.com", "db": "example", "login": "erpdoo-reader", "api_key": "k"}}
    odoo = "odoo: {url: u, db: d, login: l, api_key: k}"
    assert load_config(write(tmp_path, f"sinks: [{{type: stdout}}]\nsource: {{type: polling, {odoo}}}\n"))[
        "source"]["deletes"] == 300.0
    for text, error in [
        ("source: polling", "polling needs the Odoo to poll"),
        ("source: postgres", "is replication, or a mapping with type: polling"),
        ("source: {type: replication, slot: x}", "replication takes no settings"),
        (f"source: {{type: polling, every: 5, {odoo}}}", "unknown settings"),
        ("source: {type: polling, odoo: {url: u, db: d, login: l}}", "odoo needs api_key, db, login, url"),
        ("source: {type: polling, odoo: {url: u, db: d, login: l, api_key: ''}}", "odoo needs"),
        (f"source: {{type: polling, interval: soon, {odoo}}}", "are numbers"),
        (f"source: {{type: polling, deletes: true, {odoo}}}", "are numbers"),
        (f"source: {{type: polling, interval: 0, {odoo}}}", "must be positive"),
    ]:
        with pytest.raises(ConfigError, match=error):
            load_config(write(tmp_path, "sinks: [{type: stdout}]\n" + text))
