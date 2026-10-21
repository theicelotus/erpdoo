"""A small in-memory Odoo behind the JSON-RPC calls polling mode makes.

Records keep write_date and create_date to the microsecond, as Postgres
does, and the API gives them to the second, as Odoo does. Writes to tracked
fields add tracking messages, as Odoo's chatter does.
"""

from __future__ import annotations

from datetime import datetime, timedelta

FMT = "%Y-%m-%d %H:%M:%S"
MAGIC = [("id", "integer"), ("create_date", "datetime"), ("write_date", "datetime"),
         ("write_uid", "many2one", "res.users")]

# model -> [(name, type, relation or None, options)]
SCHEMA = {
    "sale.order": [
        ("name", "char"), ("state", "selection", None, {"tracking": 3, "selection": [
            ("draft", "Quotation"), ("sent", "Quotation Sent"), ("sale", "Sales Order"), ("cancel", "Cancelled")]}),
        ("amount_total", "monetary", None, {"currency_field": "currency_id"}),
        ("currency_id", "many2one", "res.currency"), ("partner_id", "many2one", "res.partner"),
        ("order_line", "one2many", "sale.order.line", {"relation_field": "order_id"}),
        ("tag_ids", "many2many", "crm.tag", {"relation_table": "sale_order_tag_rel", "column1": "order_id",
                                             "column2": "tag_id"}),
    ],
    "sale.order.line": [
        ("order_id", "many2one", "sale.order"), ("name", "text"), ("product_id", "many2one", "product.product"),
        ("product_uom_qty", "float", None, {"digits": [16, 2]}),
    ],
    "crm.lead": [("name", "char"), ("probability", "float"), ("active", "boolean"),
                 ("partner_id", "many2one", "res.partner")],
    "res.partner": [("name", "char")],
    "product.product": [("default_code", "char"), ("product_tmpl_id", "many2one", "product.template"),
                        ("name", "char", None, {"store": False, "related": "product_tmpl_id.name"})],
    "product.template": [("name", "char", None, {"translate": True})],
    "crm.tag": [("name", "char")],
    "res.currency": [("name", "char")],
    "res.users": [("name", "char")],
}


class FakeApi:
    def __init__(self) -> None:
        self.now = datetime(2026, 10, 21, 8, 0, 0, 250000)
        self.uid = 2
        self.login = "erpdoo-reader"
        self.calls = 0
        self.log: list[tuple[str, str]] = []
        self.writable: set[str] = set()  # models the user may change besides reading
        self.fields: list[dict] = []
        for model, fields in SCHEMA.items():
            for name, ttype, *rest in [*MAGIC, *fields]:
                relation = rest[0] if rest else None
                options = rest[1] if len(rest) > 1 else {}
                self.fields.append({
                    "id": len(self.fields) + 1, "model": model, "name": name, "ttype": ttype,
                    "relation": relation or False, "relation_field": options.get("relation_field", False),
                    "relation_table": options.get("relation_table", False), "column1": options.get("column1", False),
                    "column2": options.get("column2", False), "store": options.get("store", True),
                    "related": options.get("related", False), "translate": options.get("translate", False),
                    "tracking": options.get("tracking", 0), "write_date": datetime(2026, 1, 1), "_options": options})
        self.records: dict[str, dict[int, dict]] = {model: {} for model in SCHEMA}
        self.tracking: list[dict] = []
        self.messages: dict[int, dict] = {}
        self.currencies = {1: 2}  # id -> decimal places
        self.create("res.currency", {"name": "USD"})
        self.create("res.users", {"name": "Mitchell Admin"})

    # The database, as tests change it.

    def tick(self, seconds: float = 1.0) -> None:
        self.now += timedelta(seconds=seconds)

    def field(self, model: str, name: str) -> dict | None:
        return next((f for f in self.fields if f["model"] == model and f["name"] == name), None)

    def create(self, model: str, values: dict, uid: int = 2) -> int:
        record_id = max(self.records[model], default=0) + 1
        row = {"id": record_id, "create_date": self.now, "write_date": self.now, "write_uid": uid}
        for f in self.fields:
            if f["model"] == model and f["store"] and f["ttype"] != "one2many" and f["name"] not in row:
                row[f["name"]] = [] if f["ttype"] == "many2many" else None
        row.update(values)
        self.records[model][record_id] = row
        return record_id

    def write(self, model: str, record_id: int, values: dict, uid: int = 2, track: bool = True) -> None:
        row = self.records[model][record_id]
        tracked = {k: (row[k], v) for k, v in values.items()
                   if track and (self.field(model, k) or {}).get("tracking") and row[k] != v}
        row.update(values, write_date=self.now, write_uid=uid)
        if tracked:
            message = max(self.messages, default=0) + 1
            self.messages[message] = {"id": message, "model": model, "res_id": record_id,
                                      "date": self.now + timedelta(milliseconds=20), "create_uid": uid}
            for name, (old, new) in tracked.items():
                f = self.field(model, name)
                labels = dict(f["_options"].get("selection", []))
                value = {"id": len(self.tracking) + 1, "field_id": f["id"], "mail_message_id": message}
                for side, v in (("old", old), ("new", new)):
                    value[f"{side}_value_char"] = labels.get(v, v) if f["ttype"] in ("selection", "char") else False
                    value[f"{side}_value_integer"] = v if f["ttype"] in ("integer", "many2one", "boolean") else 0
                    value[f"{side}_value_float"] = v if f["ttype"] in ("float", "monetary") else 0.0
                    value[f"{side}_value_text"] = v if f["ttype"] == "text" else False
                    value[f"{side}_value_datetime"] = False
                self.tracking.append(value)

    def delete(self, model: str, record_id: int) -> None:
        del self.records[model][record_id]

    # The API.

    def authenticate(self) -> int:
        self.calls += 1
        return self.uid

    def call(self, model: str, method: str, *args, context=None, **kwargs):
        self.calls += 1
        self.log.append((model, method))
        if method == "fields_get":
            names = args[0] if args else None
            out = {}
            for f in self.fields:
                if f["model"] == model and (names is None or f["name"] in names):
                    desc = {"type": f["ttype"]}
                    options = f["_options"]
                    if f["ttype"] == "float" and "digits" in options:
                        desc["digits"] = options["digits"]
                    if "currency_field" in options:
                        desc["currency_field"] = options["currency_field"]
                    if "selection" in options:
                        desc["selection"] = options["selection"]
                    out[f["name"]] = desc
            return out
        if method == "has_access":
            return args[1] == "read" or model in self.writable
        if model == "res.lang":
            return [{"id": 1, "code": "en_US"}]
        if model == "res.currency" and method == "search_read":
            return [{"id": i, "decimal_places": p} for i, p in self.currencies.items()]
        rows = self._table(model)
        if method == "read":
            ids, fields = args
            return [self._out(model, rows[i], fields) for i in ids if i in rows]
        domain = args[0]
        found = [r for r in rows.values() if all(self._test(model, r, cond) for cond in domain)]
        for key in reversed((kwargs.get("order") or "id").split(",")):
            name, *desc = key.split()
            found.sort(key=lambda r: (r[name] is not None, r[name]), reverse=bool(desc))
        found = found[:kwargs["limit"]] if kwargs.get("limit") else found
        if method == "search":
            return [r["id"] for r in found]
        if method == "search_count":
            return len(found)
        return [self._out(model, r, kwargs["fields"]) for r in found]

    def _table(self, model: str) -> dict[int, dict]:
        if model == "ir.model":
            return {i: {"id": i, "model": m} for i, m in enumerate(SCHEMA, 1)}
        if model == "ir.model.fields":
            return {f["id"]: f for f in self.fields}
        if model == "mail.tracking.value":
            return {v["id"]: v for v in self.tracking}
        if model == "mail.message":
            return self.messages
        return self.records[model]

    def _value(self, model: str, row: dict, path: str):
        first, _, rest = path.partition(".")
        if rest:  # mail_message_id.date
            return self._value("mail.message", self.messages[row[first]], rest)
        return row[first]

    def _test(self, model: str, row: dict, cond) -> bool:
        name, op, value = cond
        have = self._value(model, row, name)
        if isinstance(have, datetime):
            value = datetime.strptime(value, FMT)
        if op == "=":
            return have == value
        if op == "in":
            return have in value
        if have is None:
            return False
        return {">": have > value, ">=": have >= value, "<": have < value}[op]

    def _out(self, model: str, row: dict, fields: list[str]) -> dict:
        out = {"id": row["id"]}
        for name in fields:
            f = self.field(model, name)
            if model in ("ir.model", "ir.model.fields", "mail.tracking.value", "mail.message"):
                value = row[name]
                if isinstance(value, datetime):
                    value = value.strftime(FMT)
                elif name in ("field_id", "mail_message_id", "create_uid") and value:
                    value = [value, "x"]
                out[name] = value
                continue
            if f["ttype"] == "one2many":
                child = self.records[f["relation"]]
                out[name] = sorted(i for i, r in child.items() if r[f["relation_field"]] == row["id"])
                continue
            if f["related"]:
                hop, target = f["related"].split(".")
                value = self.records[self.field(model, hop)["relation"]][row[hop]][target]
            else:
                value = row[name]
            if f["translate"] and isinstance(value, dict):
                value = value["en_US"]
            if isinstance(value, datetime):
                value = value.strftime(FMT)  # Odoo gives times to the second
            elif f["ttype"] == "many2one" and value:
                value = [value, f"{f['relation']},{value}"]
            elif f["ttype"] in ("float", "monetary"):
                value = float(value or 0.0)
            elif value is None and f["ttype"] != "many2many":
                value = False
            out[name] = value
        return out
