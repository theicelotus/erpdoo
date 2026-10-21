from datetime import datetime, timezone

from erpdoo.backfill import _UNKNOWN, _matches_now, _value, parse_time, window
from erpdoo.rules import parse_rule

UTC = timezone.utc


def test_times_are_utc_unless_they_say_otherwise():
    assert parse_time("2026-10-21T00:44:03") == datetime(2026, 10, 21, 0, 44, 3, tzinfo=UTC)
    assert parse_time("2026-10-21T00:44:03Z") == datetime(2026, 10, 21, 0, 44, 3, tzinfo=UTC)
    assert parse_time("2026-10-21T02:44:03+02:00") == datetime(2026, 10, 21, 0, 44, 3, tzinfo=UTC)


def test_window_widens_to_whole_seconds():
    # Odoo's message times have no fractions: the second "since" falls in counts.
    since, until = window(datetime(2026, 1, 1, 0, 0, 3, 747894, UTC), datetime(2026, 1, 1, 0, 0, 29, 982924, UTC))
    assert (since, until) == (datetime(2026, 1, 1, 0, 0, 3, tzinfo=UTC), datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC))


def test_tracked_values():
    state = {"type": "selection", "labels": {"Quotation": "draft", "Sales Order": "sale", "Bon de commande": "sale"}}
    row = {"old_value_char": "Quotation", "new_value_char": "Bon de commande"}
    assert (_value(state, row, "old"), _value(state, row, "new")) == ("draft", "sale")
    assert _value(state, {"old_value_char": "Renamed label"}, "old") is _UNKNOWN
    assert _value(state, {"old_value_char": None}, "old") is None
    assert _value({"type": "boolean"}, {"new_value_integer": 0}, "new") is False
    assert _value({"type": "many2one"}, {"new_value_integer": 4, "new_value_char": "Won"}, "new") == 4
    assert _value({"type": "float"}, {"new_value_float": 100.0}, "new") == 100
    assert _value({"type": "datetime"}, {"new_value_datetime": datetime(2026, 1, 1, 8, 30)}, "new") == "2026-01-01 08:30:00"


def test_current_values_against_a_rule():
    won = parse_rule("LeadWon", {"model": "crm.lead",
                                 "when": {"probability": {"from": [10, 50], "to": 100}, "active": True}})
    # "from" cannot be checked from current values; "to" and plain values can.
    assert _matches_now(won, {"probability": 100.0, "active": True})
    assert not _matches_now(won, {"probability": 100.0, "active": False})
    assert not _matches_now(won, {"probability": 50.0, "active": True})
