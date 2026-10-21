# Slot health

Postgres keeps all WAL that erpdoo's replication slot has not confirmed.
While erpdoo keeps up, that is a few seconds' worth; if erpdoo is stopped,
or a sink blocks the stream, it grows until erpdoo catches up. This page
covers how erpdoo reports the slot's lag, how to keep a slot left behind
from filling the disk, and what happens when Postgres invalidates the slot.

- [Lag reports](#lag-reports)
- [Limit what a slot can hold](#limit-what-a-slot-can-hold)
- [When Postgres invalidates the slot](#when-postgres-invalidates-the-slot)
- [Removing erpdoo](#removing-erpdoo)

## Lag reports

Every minute erpdoo logs how far the slot is behind, in bytes of WAL and in
seconds (Postgres' `flush_lag` for erpdoo's connection: how long WAL waits
before erpdoo confirms it; erpdoo confirms at least every 10 seconds):

```
INFO erpdoo: slot erpdoo: 469.7 kB of WAL not confirmed yet, 0.4 s behind; 12 rows fetched on demand so far
```

It logs a warning instead when the slot is more than 1 GB or 300 seconds
behind, or when Postgres is about to invalidate it. The thresholds are set
in [erpdoo.yml](../erpdoo.yml):

```yaml
slot:
  lag_warning: 1GB          # bytes of WAL, or 512MB, 2GB, ...
  lag_warning_seconds: 300
  report_seconds: 60
```

The [live viewer](architecture.md#live-viewer)'s header shows the same lag.

## Limit what a slot can hold

By default Postgres keeps WAL for a slot without limit, so a slot left
behind (erpdoo stopped for days) can fill the disk and stop the database,
Odoo included. Set `max_slot_wal_keep_size` in `postgresql.conf` (or with
`ALTER SYSTEM SET max_slot_wal_keep_size = '10GB'` and
`SELECT pg_reload_conf()`, no restart needed) to cap it.

Pick a size that covers the longest erpdoo outage you want to survive: when
a slot needs more, Postgres invalidates it at the next checkpoint and
removes the WAL. With the cap set, erpdoo's lag line also says how much
more the slot can fall behind (`invalidated after 31.1 MB more`) and warns
when Postgres marks it as about to be invalidated.

## When Postgres invalidates the slot

If Postgres has invalidated the slot, erpdoo creates it again when it
starts, copies its state store again and logs the time window it could
not stream, from the last transaction it processed to the new slot:

```
ERROR erpdoo: replication slot erpdoo had been invalidated by Postgres, which removed WAL erpdoo had not read yet (it held more than max_slot_wal_keep_size); created it again. Changes committed between 2026-10-21T01:28:55.063437+00:00 and 2026-10-21T01:29:05.901836+00:00 were not streamed, so events from that time may be missing. They are backfilled from Odoo's records next. The state store is copied again.
INFO erpdoo: backfilled 2026-10-21T01:28:55.063437+00:00 to 2026-10-21T01:29:05.901836+00:00: 3 events (2 from Odoo's field tracking, 1 approximate)
```

It then rebuilds the events of that window from Odoo's own records (see
[Backfill](backfill.md), exact only for fields Odoo tracks) before it
streams again. To only log the window, turn that off in erpdoo.yml:

```yaml
backfill:
  after_lost_slot: false
```

[scripts/backfill_test.py](../scripts/backfill_test.py) checks this on the
dev stack (see [CONTRIBUTING.md](../CONTRIBUTING.md#the-checks)).

## Removing erpdoo

If you remove erpdoo, drop its slot (`SELECT pg_drop_replication_slot('erpdoo')`),
or Postgres keeps WAL for it.
