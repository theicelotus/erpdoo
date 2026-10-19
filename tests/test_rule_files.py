from erpdoo.rules import load_rules


def test_repository_rules_parse():
    names = {r.name for r in load_rules("rules")}
    assert "SaleOrderConfirmed" in names
