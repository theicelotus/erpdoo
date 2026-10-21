import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

import fake_odoo
from erpdoo.inbound import (
    MessageError, apply, load_inbound_rules, parse_inbound_rule, render, validate_inbound,
)
from erpdoo.odoo_rpc import OdooClient, OdooError, OdooSetupError, OdooUnavailable
from erpdoo.rules import RuleError

RULES = Path(__file__).parent.parent / "rules" / "inbound"

CONFIRM = parse_inbound_rule("OrderAccepted", {
    "topic": "shop.events", "model": "sale.order", "find": {"name": "{order.ref}", "state": ["draft", "sent"]},
    "already_done": {"state": "sale"},
    "steps": [{"write": {"client_order_ref": "{order.ref}-{id}"}},
              {"call": "action_confirm", "context": {"send_email": False}},
              {"wizard": "sale.advance.payment.inv", "values": {"amount": "{total}"}, "call": "create_invoices"}],
    "expect": {"state": "sale"},
})


class FakeOdoo:
    """Answers search with ``found``, read with ``row``, other calls with ``answers``."""

    def __init__(self, found=(5,), done=(), row=None, answers=None):
        self.found, self.done, self.row, self.answers = list(found), list(done), row or {"state": "sale"}, answers or {}
        self.calls = []

    def call(self, model, method, *args, context=None, **kwargs):
        self.calls.append((model, method, args, context, kwargs))
        if method == "search":
            return self.done if (model, method) in [c[:2] for c in self.calls[:-1]] else self.found
        if method == "read":
            return [self.row]
        if method == "create":
            return 77
        return self.answers.get(method, True)


MESSAGE = {"id": "m-1", "type": "OrderAccepted", "order": {"ref": "S00042"}, "total": 12.5}


def test_render_keeps_types_and_formats_strings():
    assert render({"a": "{total}", "b": "#{order.ref}", "c": ["{record.id}"], "d": 3}, MESSAGE, 9) == {
        "a": 12.5, "b": "#S00042", "c": [9], "d": 3}
    with pytest.raises(MessageError, match="the message has no order.missing"):
        render("{order.missing}", MESSAGE)


def test_apply_finds_the_record_and_runs_the_steps():
    odoo = FakeOdoo()
    outcome = apply(CONFIRM, MESSAGE, odoo)
    assert (outcome.status, outcome.record_id) == ("done", 5)
    assert [(c[0], c[1]) for c in odoo.calls] == [
        ("sale.order", "search"), ("sale.order", "search"), ("sale.order", "write"), ("sale.order", "action_confirm"),
        ("sale.advance.payment.inv", "create"), ("sale.advance.payment.inv", "create_invoices"), ("sale.order", "read")]
    assert odoo.calls[0][2] == ([("name", "=", "S00042"), ("state", "in", ["draft", "sent"])],)
    assert odoo.calls[1][2] == ([("id", "=", 5), ("state", "=", "sale")],)  # already done?
    assert odoo.calls[2][2] == ([5], {"client_order_ref": "S00042-m-1"})
    assert odoo.calls[4][2] == ({"amount": 12.5},)
    assert odoo.calls[4][3] == {"active_model": "sale.order", "active_id": 5, "active_ids": [5]}
    assert odoo.calls[5][2] == ([77],)


def test_already_done_skips_the_steps():
    odoo = FakeOdoo(done=[5])
    assert apply(CONFIRM, MESSAGE, odoo).status == "already_done"
    assert [c[1] for c in odoo.calls] == ["search", "search"]


def test_no_record_or_several_are_permanent():
    with pytest.raises(MessageError, match="no sale.order with name = 'S00042', state = \\['draft', 'sent'\\]"):
        apply(CONFIRM, MESSAGE, FakeOdoo(found=[]))
    with pytest.raises(MessageError, match="more than one sale.order"):
        apply(CONFIRM, MESSAGE, FakeOdoo(found=[5, 6]))


def test_expect_names_a_wizard_odoo_asked_for():
    odoo = FakeOdoo(row={"state": "draft"}, answers={"create_invoices": {"type": "ir.actions.act_window",
                                                                         "res_model": "sale.advance.payment.inv"}})
    with pytest.raises(MessageError, match=r"has \{'state': 'draft'\}, expected \{'state': 'sale'\}"
                                           r" \(Odoo asked for sale.advance.payment.inv\)"):
        apply(CONFIRM, MESSAGE, odoo)


def test_already_done_in_another_model():
    rule = parse_inbound_rule("PaymentReceived", {
        "topic": "p", "model": "account.move", "find": {"name": "{invoice}"},
        "already_done": {"model": "account.payment", "find": {"memo": "{payment_id}"}},
        "steps": [{"call": "action_register_payment"}]})
    odoo = FakeOdoo(done=[3])
    assert apply(rule, {"invoice": "INV/1", "payment_id": "pi_1"}, odoo).status == "already_done"
    assert odoo.calls[1][:3] == ("account.payment", "search", ([("memo", "=", "pi_1")],))


def test_rule_mistakes_are_named():
    base = {"topic": "t", "model": "sale.order", "find": {"name": "{n}"}, "steps": [{"call": "action_confirm"}]}
    for change, error in [
        ({"steps": [{"call": "_action_done"}]}, "a public method name"),
        ({"steps": [{"write": {}}]}, "write takes fields and values"),
        ({"steps": [{"write": {"a": 1}, "call": "x"}]}, "one of write, call or wizard"),
        ({"retries": 3}, "unknown keys \\['retries'\\]"),
        ({"find": None}, "find must map fields to values"),
    ]:
        with pytest.raises(RuleError, match=error):
            parse_inbound_rule("R", {**base, **change})
    with pytest.raises(RuleError, match="needs topic, model, find and steps"):
        parse_inbound_rule("R", {"model": "sale.order"})


def test_validate_against_odoo_models():
    registry = fake_odoo.registry()
    rules = [parse_inbound_rule("A", {"topic": "t", "model": "sale.order", "find": {"name": "{n}"},
                                      "steps": [{"write": {"nope": 1}}]}),
             parse_inbound_rule("B", {"topic": "t", "model": "stock.picking", "find": {"name": "{n}"},
                                      "steps": [{"call": "button_validate"}]})]
    with pytest.raises(RuleError, match="inbound rule A: write: sale.order has no field nope"):
        validate_inbound(rules, registry)
    active, skipped = validate_inbound(rules[1:], registry)
    assert active == [] and "model stock.picking not found" in skipped[0]


def test_shipped_rules_parse():
    rules = {r.name: r for r in load_inbound_rules(RULES)}
    assert {(r.name, r.topic) for r in rules.values()} == {
        ("DeliveryShipped", "warehouse.events"), ("PaymentReceived", "payments.events")}
    assert rules["DeliveryShipped"].matches({"type": "DeliveryShipped"})
    assert not rules["DeliveryShipped"].matches({"type": "PaymentReceived"}) and not rules["DeliveryShipped"].matches({})


@pytest.fixture
def odoo_server():
    """Answers /jsonrpc with state["reply"] (a dict, or an HTTP status)."""
    state = {"reply": {"result": 7}}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            reply = state["reply"]
            body = json.dumps({"jsonrpc": "2.0", "id": 1, **reply}).encode() if isinstance(reply, dict) else b""
            self.send_response(200 if isinstance(reply, dict) else reply)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    state["url"] = f"http://127.0.0.1:{server.server_port}"
    yield state
    server.shutdown()


def error(name, message="m"):
    return {"error": {"code": 200, "message": "Odoo Server Error", "data": {"name": name, "message": message}}}


def test_odoo_errors_are_sorted_by_what_to_do(odoo_server):
    client = OdooClient(odoo_server["url"], "odoo", "bot", "key")
    assert client.call("sale.order", "search", []) == 7 and client.uid == 7
    for reply, kind in [
        (error("odoo.exceptions.UserError", "nothing left to pay"), OdooError),
        (error("odoo.exceptions.ValidationError"), OdooError),
        (error("odoo.exceptions.AccessError"), OdooSetupError),
        (error("psycopg2.errors.SerializationFailure"), OdooUnavailable),
        (503, OdooUnavailable),
        (404, OdooSetupError),
    ]:
        odoo_server["reply"] = reply
        with pytest.raises(kind):
            client.call("sale.order", "write", [1], {})
    odoo_server["reply"] = {"result": False}
    with pytest.raises(OdooSetupError, match="refused the API key of user bot"):
        OdooClient(odoo_server["url"], "odoo", "bot", "wrong").authenticate()
    with pytest.raises(OdooUnavailable):
        OdooClient("http://127.0.0.1:1", "odoo", "bot", "key", timeout=1).authenticate()
