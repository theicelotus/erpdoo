# Performance

Measured latency and throughput on the dev stack, the state store's size
and startup time, and the WAL an order confirmation writes. All numbers are
from one laptop, with the scripts that measured them.

- [Latency and throughput](#latency-and-throughput)
  - [How to read it](#how-to-read-it)
  - [Reproducing](#reproducing)
- [State store size and startup](#state-store-size-and-startup)
- [WAL per order confirmation](#wal-per-order-confirmation)

## Latency and throughput

Measured with [scripts/benchmark.py](../scripts/benchmark.py) on the dev
stack, everything on one laptop: Intel Core i9-11900H (8 cores, 16
threads), 15.4 GiB RAM, Samsung NVMe SSD, Fedora 42 (kernel 6.19), Docker
29.5.3. Odoo, Postgres, Redpanda, Redpanda Console and erpdoo all share
that machine. erpdoo runs the six shipped rules with the dev config (two
Kafka sinks, JSON Schema and Avro with the schema registry, and stdout;
viewer on), and each event includes the partner and order lines from the
state store. Latency is from the Postgres commit to the moment a Kafka
consumer of the JSON Schema topic receives the event. Results from two
consecutive runs:

| | Run 1 | Run 2 |
|---|---|---|
| Latency, one order at a time (200 orders), p50 / p95 / p99 | 18.4 / 20.2 / 30.1 ms | 18.5 / 21.9 / 50.2 ms |
| Burst while erpdoo runs (3,000 orders, 25 per transaction, 8 concurrent clients) | 159 events/s | 157 events/s |
| Latency during that burst, p50 / p95 / p99 | 30 / 89 / 95 ms | 36 / 90 / 101 ms |
| Last event after the last commit | 0.05 s | 0.05 s |
| Backlog of 3,000 confirmed orders, erpdoo started afterwards | 2,993 events/s (1.00 s) | 2,520 events/s (1.19 s) |
| erpdoo startup (container start to streaming) | 0.89 s | 0.93 s |

### How to read it

- In the burst, Odoo is the limit: it committed about 160 confirmations a
  second, and erpdoo delivered each within about 100 ms, finishing 50 ms
  after the last commit.
- The backlog run takes Odoo out of the picture: erpdoo works through 3,000
  waiting confirmations at 2,500 to 3,000 events a second, each written to
  both topics, including the state store lookups for each event's partner
  and order lines. The orders were created while erpdoo ran, so their
  lines were already in the store; orders from before erpdoo started cost
  one query each for their lines.
- No events were missing or duplicated in any phase.
- Schemas cost no measurable time. An earlier measurement, with one Kafka
  sink writing plain JSON, gave 13.2 ms at p50; that same configuration
  gives 18.8 ms on this machine today (and one JSON Schema sink alone
  18.6 ms), with a dev database grown to about 47,000 orders by the test
  runs. The difference is the machine and its data, not the formats.
- The orders use a service product, so no deliveries are created.

Limits of these numbers: this is one machine with no network between the
services, and small demo tables. Expect higher latency across real
networks, and a longer first start on a large database while the state
store copies the rule tables.

### Reproducing

Start the stack and run `.venv/bin/python scripts/benchmark.py`. It creates
about 6,200 orders in the dev database and stops and starts the erpdoo
container for the backlog phase.

## State store size and startup

The state store keeps the rule tables with only the columns the rules use,
and fetches related records when an event first needs them (see
[The state store](architecture.md#the-state-store)). Before, it copied
every column of the rule tables and of every related table at startup.
Measured on the dev database after the runs above (about 16,000 sale
orders and order lines, 3,700 deliveries and stock moves), each with a new
store and its own slot, two runs each:

| | Copy whole tables | Columns rules use, related rows on demand |
|---|---|---|
| Startup, process start to streaming | 2.15 s / 1.90 s | 0.94 s / 0.92 s |
| Store size after startup (SQLite file after a clean stop) | 59.2 MB | 8.5 MB |
| The same after the smoke test ran meanwhile | | 8.5 MB |

Startup is dominated by copying `sale_order` (16,000 rows), now with 13 of
its 51 columns; `sale_order_line` and the other related tables are no
longer copied.

## WAL per order confirmation

Earlier versions of erpdoo needed `REPLICA IDENTITY FULL` on the rule
tables, so that Postgres wrote each updated row's previous version into the
WAL. erpdoo now takes that row from its state store instead (see
[Replica identity](postgres-setup.md#replica-identity)). Measured with
[scripts/wal_cost.py](../scripts/wal_cost.py) on the same machine: 300 sale
orders, each with one stocked product (so confirming also creates a
delivery), confirmed one at a time through Odoo's API, three runs each.

| | `REPLICA IDENTITY FULL` | Default (primary key) |
|---|---|---|
| WAL on the rule tables (`sale_order`, `stock_picking`, ...) | 1,356 / 1,357 / 1,403 bytes | 963 / 966 / 986 bytes |
| All WAL, without full-page images | 15,956 / 15,966 / 17,455 bytes | 15,657 / 15,734 / 17,212 bytes |
| All WAL, with full-page images | 16,166 / 16,151 / 18,268 bytes | 22,477 / 16,041 / 19,603 bytes |

Dropping `REPLICA IDENTITY FULL` saves about 390 bytes per confirmation,
29% of the WAL written to the rule tables. That is only about 2% of all the
WAL a confirmation writes: most of it goes to Odoo's chatter
(`mail_message`, `mail_followers`) and to indexes. The totals vary from run
to run by more than the saving, mostly through full-page images, which
depend on when checkpoints happen; the rule-table figure is the steady one.

To measure the current setup, run `.venv/bin/python scripts/wal_cost.py
--orders 300` on the dev stack. It reads the WAL with `pg_walinspect`, which
it installs in the `postgres` database (not Odoo's) and which needs a
superuser.
