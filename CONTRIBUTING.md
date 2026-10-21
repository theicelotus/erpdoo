# Contributing

Thanks for helping with erpdoo. This page covers setting up the dev stack,
the checks every change must pass, and what each of them tests. Most real
bugs are in how Postgres, Odoo and the sinks interact, so changes are
checked against the running stack, not only with unit tests.

- [Setting up](#setting-up)
- [Running the checks](#running-the-checks)
- [The checks](#the-checks)
  - [Unit tests](#unit-tests)
  - [Smoke test](#smoke-test)
  - [Replay test](#replay-test)
  - [Delivery test](#delivery-test)
  - [Inbound test](#inbound-test)
  - [Custom table test](#custom-table-test)
  - [Backfill test](#backfill-test)
  - [Late app test](#late-app-test)
  - [Polling test](#polling-test)
- [Benchmarks](#benchmarks)
- [Pull requests](#pull-requests)

## Setting up

Start the dev stack (see [Quick start](README.md#quick-start)) and install
erpdoo with its development extras in a virtual environment:

```sh
docker compose up -d --build
python -m venv .venv && .venv/bin/pip install -e '.[dev,viewer]'
```

The `dev` extra has pytest and the libraries the test scripts use
(confluent-kafka's own deserializers, `jsonschema`); `viewer` is needed for
the viewer's unit tests.

## Running the checks

```sh
scripts/check_all.sh
```

[scripts/check_all.sh](scripts/check_all.sh) rebuilds erpdoo
(`docker compose up -d --build erpdoo`) and runs every check below, unit
tests and test scripts, one after the other against the dev stack, with a
summary at the end. It exits non-zero if any check failed. `PYTHON`
overrides the interpreter (default: `.venv/bin/python` if it exists).

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs the same
checks on every push and pull request, as separate steps: the unit tests on
Python 3.11 and 3.13, and the scripts against a full stack. A new check
goes into both `check_all.sh` and `ci.yml`.

## The checks

### Unit tests

```sh
.venv/bin/pytest -q
```

The tests in [tests/](tests/) need no stack: they use a small fake Odoo
schema and API, and pgoutput messages built in the test.

### Smoke test

```sh
.venv/bin/python scripts/smoke_test.py
```

[scripts/smoke_test.py](scripts/smoke_test.py) checks the whole path:

- it drives every shipped event through Odoo's API, and checks actions that
  must not produce an event;
- it reads every event from both topics with confluent-kafka's own
  deserializers; each must be valid against the schema registered for its
  event type, and the Avro event must say the same as the JSON one;
- it compares every field of every event, related records and many2many
  lines included, with the value Odoo returns for it through JSON-RPC, by
  Odoo's own field types (see [scripts/event_checks.py](scripts/event_checks.py)).
  The comparison must cover booleans, dates, datetimes, numbers,
  selections and translated fields (the product gets a French name, and
  the event must show the English one). With the bug that once read
  booleans from snapshots as false put back, it fails with
  `record.partner_id.is_company: False, Odoo says True (boolean)`; the
  schema check alone passes, since `false` is a valid boolean;
- it checks that erpdoo's role cannot write to Odoo's tables (its first
  line: "erpdoo's role is read-only").

### Replay test

[scripts/replay_test.py](scripts/replay_test.py) checks the crash case:
erpdoo delivers an event, the customer is renamed, and erpdoo crashes
before confirming the slot. The event sent again after the restart must be
identical to the first, customer name included. It uses a test hook
(`ERPDOO_TEST_HOLD_ACK` in [docker-compose.yml](docker-compose.yml), which
does nothing unless its file exists).

### Delivery test

[scripts/delivery_test.py](scripts/delivery_test.py) checks outages and
crashes (see [Delivery](docs/sinks.md#delivery)), with the dev stack's
erpdoo (at-least-once, because of its stdout sink) and a second one whose
sinks are all Kafka (exactly once).

- It confirms orders while both are stopped, then while Redpanda is down.
  Each event must arrive exactly once on every topic once the service is
  back, and while Redpanda is down neither replication slot may be
  confirmed past any of the undelivered transactions.
- Then the second erpdoo crashes after a Kafka commit, before confirming
  its slot, and once before the commit (the test hook): a `read_committed`
  consumer must get each event exactly once, and none of the uncommitted
  ones while it holds.
- With erpdoo changed to confirm the slot before delivering, it fails,
  naming the order whose event was lost; with `--no-transactions` (the
  second erpdoo at-least-once) the two crashes fail it with duplicates.

It takes about four minutes.

### Inbound test

[scripts/inbound_test.py](scripts/inbound_test.py) sends both kinds of
[inbound](docs/inbound.md) messages on the dev stack and checks the outcome
in Odoo, the marker on the events and the stdout sink leaving out the
warehouse's, a repeated message, the same payment in a new message, a
message for a missing invoice (dead-lettered), and a payment sent while
Odoo is stopped (carried out once, after it is back).

### Custom table test

[scripts/custom_table_test.py](scripts/custom_table_test.py) installs a
small test module ([tests/odoo_addons](tests/odoo_addons)) whose model sets
its own `_table`, and checks that erpdoo finds the table both by its
columns and through `tables:` in erpdoo.yml (see
[Tables of custom models](docs/postgres-setup.md#tables-of-custom-models)).

### Backfill test

[scripts/backfill_test.py](scripts/backfill_test.py) checks the
[backfill after a lost slot](docs/slot-health.md#when-postgres-invalidates-the-slot):
with `max_slot_wal_keep_size = '64MB'` and erpdoo stopped, it confirms an
order, cancels another and wins a lead, writes WAL elsewhere until Postgres
invalidates the slot, and expects each change exactly once as a backfilled
event that fits its registered schema.

### Late app test

[scripts/late_app_test.py](scripts/late_app_test.py) checks
[apps installed later](docs/backfill.md#apps-installed-later) in both
modes. It runs a replication and a polling erpdoo with rules for a test app
([tests/odoo_addons/erpdoo_late_app](tests/odoo_addons/erpdoo_late_app)),
installs the app while they run, and opens and escalates tickets (Odoo
tracks a ticket's state, not its priority) before the replication erpdoo's
admin SQL is run and while the polling one is paused. Each change must
arrive once, backfilled and valid against its schema, with the same ids in
both modes for tracked changes; a ticket opened afterwards must be streamed
(or polled), and restarting both must send nothing again.

### Polling test

[scripts/polling_test.py](scripts/polling_test.py) runs a second erpdoo in
[polling mode](docs/polling.md) next to the dev stack's (with its own
topics, schema namespaces and state, and no way to reach Postgres), drives
the smoke test's scenarios through Odoo one poll at a time, and compares
its events with replication mode's: the same types, records and
transitions, each event valid against its schema in both formats and equal
to Odoo's values. It also restarts the polling erpdoo in the middle (no
gap, nothing sent twice), runs a 15 s transaction that commits after the
cursor passed it, pauses erpdoo while an invoice is posted and paid and a
lead is won and lost, has it carry out an inbound message, deletes a lead,
and checks its schemas equal replication mode's. It prints the expected
differences:

```
expected difference: mark opportunity won: approximate (Odoo does not track probability), "from" is the previous poll's
expected difference: post and pay between two polls: record.amount_residual: '345.00' in replication, '0.00' polled (not tracked by Odoo: as of the poll, after the payment)
expected difference: win and lose a lead between two polls: replication gave ['LeadWon'], polling nothing (the win is a state of an untracked field between two polls)
expected difference: inbound message: only the erpdoo that carried it out marks its event
```

## Benchmarks

[scripts/benchmark.py](scripts/benchmark.py) (latency and throughput) and
[scripts/wal_cost.py](scripts/wal_cost.py) (WAL per confirmation) measure
the numbers in [Performance](docs/performance.md), which says how to run
them. They are not part of the checks.

## Pull requests

- Run `scripts/check_all.sh` and read its summary before you open a pull
  request; CI runs the same checks.
- Keep commits small and focused, with clear messages.
- Keep the docs honest about trade-offs and limits, in the page each one
  belongs to.
