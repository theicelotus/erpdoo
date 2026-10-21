import pytest

from erpdoo.config import ConfigError, load_config
from erpdoo.health import DEFAULTS, SlotHealth, format_size, parse_size


def test_sizes():
    assert parse_size("1GB") == 1024**3
    assert parse_size("512 MB") == 512 * 1024**2
    assert parse_size("64kB") == 64 * 1024
    assert parse_size(1000) == 1000
    with pytest.raises(ValueError):
        parse_size("lots")
    assert format_size(1536) == "1.5 kB"
    assert format_size(3 * 1024**3) == "3.0 GB"


def test_warnings_past_thresholds_and_before_invalidation():
    settings = {**DEFAULTS, "lag_warning": 1024**2, "lag_warning_seconds": 60}
    fine = SlotHealth("erpdoo", 1000, 0.2, "reserved", None)
    assert fine.warnings(settings) == []
    assert fine.describe() == "slot erpdoo: 1000 B of WAL not confirmed yet, 0.2 s behind"
    late = SlotHealth("erpdoo", 2 * 1024**2, 90.0, "unreserved", 0)
    assert late.warnings(settings) == [
        "more than 1.0 MB of WAL behind", "more than 60 s behind",
        "Postgres is about to invalidate the slot (max_slot_wal_keep_size)"]
    assert "invalidated after 0 B more" in late.describe()
    assert "time behind not measured" in SlotHealth("erpdoo", 0, None, "reserved", None).describe()


def test_slot_settings(tmp_path):
    path = tmp_path / "erpdoo.yml"
    path.write_text("sinks: [{type: stdout}]\nslot: {lag_warning: 2GB, lag_warning_seconds: 120}\n")
    assert load_config(path)["slot"] == {"lag_warning": 2 * 1024**3, "lag_warning_seconds": 120, "report_seconds": 60}
    path.write_text("sinks: [{type: stdout}]\n")
    assert load_config(path)["slot"] == DEFAULTS
    path.write_text("sinks: [{type: stdout}]\nslot: {lag_warning: lots}\n")
    with pytest.raises(ConfigError, match="not a size"):
        load_config(path)
