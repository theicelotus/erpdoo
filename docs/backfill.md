# Backfill

erpdoo can rebuild the events its rules would have produced in a past time
window from what Odoo recorded, rather than from the WAL. It does this by
itself after Postgres invalidates its replication slot and for apps
installed later, and on demand with `erpdoo backfill`. This page covers how
backfilled events are built, how they differ from streamed ones, and their
limits.

- [Running a backfill](#running-a-backfill)
- [How events are rebuilt](#how-events-are-rebuilt)
- [Limits](#limits)
- [Apps installed later](#apps-installed-later)

## Running a backfill

```sh
erpdoo backfill --since 2026-10-01T00:00:00Z --until 2026-10-02T00:00:00Z [--dry-run]
```

rebuilds the events the active rules would have produced in that window
and sends them to the configured sinks (`--dry-run` prints them instead).
Times are ISO 8601, UTC if no zone is given. In the dev stack:
`docker compose exec erpdoo erpdoo backfill ...`.

erpdoo runs it by itself for the window of an invalidated slot (see
[Slot health](slot-health.md#when-postgres-invalidates-the-slot)), and for
rules that activate late ([below](#apps-installed-later)). It reads what
Odoo recorded, not the WAL, so it also covers time erpdoo was not running
at all. It does not use Kafka transactions, so it cannot fence a running
erpdoo (see [Delivery](sinks.md#exactly-once-to-kafka)). It reads
Postgres, so it is not available in [polling mode](polling.md), which
catches up by itself.

## How events are rebuilt

- **Rules on fields Odoo tracks** (those whose changes appear in a
  record's chatter: a sale order's or invoice's `state`, an invoice's
  `payment_state`, a delivery's `state`): each tracked change in the
  window is checked, with its real old and new values from
  `mail_tracking_value` and its time from `mail_message`. The event's
  `occurred_at` is that time and `changes` has the real `from` and `to`.
- **Other rules** (a field Odoo does not track, such as `crm.lead`'s
  `probability`, which `LeadWon` uses; create rules): erpdoo takes the
  records written (created) in the window whose current values match the
  rule. The previous values are unknown, so `from` conditions are not
  checked and `changes` has `"from": null`. These events are marked
  `"approximate": true`.

Each backfilled event says so in its source, and has an id built from the
rule, the record and the tracking message (or the record's
`write_date`), so running the same backfill again sends the same ids:

```json
"source": {"table": "sale_order", "backfill": true, "approximate": false, "mail_message_id": 65293}
```

## Limits

- **Other fields are as they are now.** The tracked change's own fields
  have the values of that moment, but the rest of the record and its
  related records (customer, lines) are read as they are when the
  backfill runs.
- **Ids differ from streamed events.** A change that was streamed and is
  also in a backfill window is sent twice, with two ids. erpdoo widens the
  window to whole seconds (Odoo stores times without fractions), so a
  change in the first second of a lost slot's backfill can be one. The
  backfill of a rule that activates late has no such overlap
  ([below](#apps-installed-later)).
- **Approximate events can be wrong.** A record written in the window for
  another reason, one that already matched the rule before, gives an
  event too: a lead won last month and edited today counts as won today,
  and an Odoo module update that rewrites records makes them all count.
  A record that matched in the window but no longer does gives none, and
  since its id comes from the `write_date`, writing the record again
  changes the id.
- **Only one event per tracked change, and only what Odoo tracked.** A
  change made without tracking (SQL, some imports, a record created
  directly in its final state) is not found. Delete rules cannot be
  backfilled, and deleted records are skipped.
- **It needs read access to Odoo's tracking:** `mail_tracking_value`,
  `ir_model_fields_selection`, and four columns of `mail_message` (the
  record and the time, not the message text), and for apps installed later
  `ir_model_data`; see the [SQL](postgres-setup.md#the-sql).
- Backfilled events are not marked as caused by an inbound message (see
  [Loop safety](inbound.md#loop-safety)).

## Apps installed later

A rule whose app is installed after erpdoo started streaming, while it runs
or while it is stopped, activates right away or once an admin has run the
SQL erpdoo logs (see
[Setting up Postgres](postgres-setup.md#your-own-rules-and-apps-installed-later)).
erpdoo then backfills the rule from when the app's model was created up to
the moment it starts streaming the model's table (polling mode does the
same through Odoo's API, see [How it works](polling.md#how-it-works)):

```
INFO erpdoo: erpdoo.test.ticket was created at 2026-10-21T12:22:59.310396+00:00 (its app was installed since erpdoo started streaming); backfilled TicketOpened, TicketEscalated, up to 2026-10-21T12:23:30.226393+00:00 when erpdoo copied its table: 4 events
```

- **No gap and no overlap.** erpdoo copies the new table into its state
  store and reads the backfill in one Postgres snapshot. A transaction that
  snapshot saw committed is backfilled; any other is checked against the
  rule when it is streamed. Which side a transaction is on comes from the
  snapshot's transaction ids, not from times, so each change is sent once.
- **When the model was created** comes from Odoo's external ids
  (`ir_model_data`, created when the app is installed); Odoo leaves
  `ir_model`'s own `create_date` empty. Records the install itself creates,
  such as demo data, are included.
- **Not for older models.** A rule added to the YAML for an app that was
  installed before erpdoo started streaming is not backfilled: run `erpdoo
  backfill` for the window you want.
- **Crash safe.** The backfilled events are saved in the state store in the
  same commit as the table's copy, and sent before anything streamed after
  it. If erpdoo stops before they are delivered, it sends them when it starts.
- The events are like any other backfilled ones, with the
  [limits](#limits) above. Without read access to `ir_model_data`, erpdoo
  logs the grant it needs and the `erpdoo backfill` command to run once
  granted, and streams on.

[scripts/late_app_test.py](../scripts/late_app_test.py) checks this in both
modes, and [scripts/backfill_test.py](../scripts/backfill_test.py) the
backfill after a lost slot (see [CONTRIBUTING.md](../CONTRIBUTING.md#the-checks)).
