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
