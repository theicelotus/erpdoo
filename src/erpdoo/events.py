"""Business events built from committed transactions.

For now there is a single hard-coded event, SaleOrderConfirmed. The YAML rule
engine (Phase 1, step 3) replaces this module's detection logic.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from erpdoo.pgoutput import format_lsn
from erpdoo.replication import Transaction

log = logging.getLogger(__name__)

# Fixed namespace so the same transaction + rule + record always gives the same id.
EVENT_ID_NAMESPACE = uuid.UUID("6f1c3a52-2b0e-4d5c-9a51-0e7d1f6b8c34")


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    model: str
    record_id: int
    payload: dict

    @property
    def key(self) -> str:
        """Kafka key: keeps all events of one record in order on one partition."""
        return f"{self.model}:{self.record_id}"


def event_id(txn: Transaction, rule: str, model: str, record_id: int) -> str:
    # xid alone can wrap around on a long-lived database; with the commit LSN
    # the transaction identity is unique.
    name = f"{txn.xid}/{format_lsn(txn.commit_lsn)}/{rule}/{model}/{record_id}"
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, name))


def net_changes(txn: Transaction, table: str) -> dict[int, tuple[dict | None, dict | None]]:
    """Per record id, the row at the start and at the end of the transaction.

    before is {} if the record was created in this transaction, and None if it
    is unknown (an update without REPLICA IDENTITY FULL). after is None if the
    record was deleted.
    """
    result: dict[int, tuple[dict | None, dict | None]] = {}
    for change in txn.changes:
        if change.table != table or change.op == "truncate":
            continue
        record_id = (change.new or change.old)["id"]
        if record_id in result:
            before, after = result[record_id]
        elif change.op == "insert":
            before, after = {}, {}
        elif change.old is not None and not change.old_is_key_only:
            before, after = change.old, dict(change.old)
        else:
            before, after = None, {}
        if change.op == "delete":
            after = None
        else:
            # Columns left out of the new row (unchanged TOAST) keep their earlier value.
            after = {**(after or {}), **change.new}
        result[record_id] = (before, after)
    return result


SALE_ORDER_FIELDS = (
    "id", "name", "state", "partner_id", "partner_invoice_id", "partner_shipping_id",
    "user_id", "team_id", "company_id", "currency_id", "pricelist_id", "date_order",
    "client_order_ref", "amount_untaxed", "amount_tax", "amount_total",
)


def sale_order_confirmed(txn: Transaction) -> list[Event]:
    """A sale order whose state became 'sale' in this transaction.

    Odoo confirms quotations from both 'draft' and 'sent' (quotation emailed),
    so any previous state other than 'sale' counts.
    """
    events = []
    for record_id, (before, after) in net_changes(txn, "sale_order").items():
        if after is None or after.get("state") != "sale":
            continue
        if before is None:
            log.warning("sale_order %s: previous state unknown, needs REPLICA IDENTITY FULL", record_id)
            continue
        previous = before.get("state")  # None: created already confirmed
        if previous == "sale":
            continue
        rule = "SaleOrderConfirmed"
        eid = event_id(txn, rule, "sale.order", record_id)
        payload = {
            "event_id": eid,
            "event_type": rule,
            "model": "sale.order",
            "record_id": record_id,
            "occurred_at": txn.commit_time.isoformat(),
            "changes": {"state": {"from": previous, "to": "sale"}},
            "record": {f: after.get(f) for f in SALE_ORDER_FIELDS},
            "source": {"table": "sale_order", "xid": txn.xid, "commit_lsn": format_lsn(txn.commit_lsn)},
        }
        events.append(Event(eid, rule, "sale.order", record_id, payload))
    return events
