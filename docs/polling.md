# Polling mode

When erpdoo cannot reach Odoo's Postgres (hosted Odoo, such as Odoo Online
or Odoo.sh, usually gives no replication access), it can read the changes
through Odoo's external JSON-RPC API instead, as a read-only Odoo user. It
then needs no Postgres access at all. The same rules, sinks, schemas,
inbound rules and live viewer work, with the differences and limits below.

- [Turning it on](#turning-it-on)
- [The two modes](#the-two-modes)
- [API access](#api-access)
- [The read-only user](#the-read-only-user)
- [How it works](#how-it-works)
- [Load on Odoo](#load-on-odoo)
- [Limits](#limits)
- [Trying it on the dev stack](#trying-it-on-the-dev-stack)

## Turning it on

With `source:` in [erpdoo.yml](../erpdoo.yml):

```yaml
source:
  type: polling
  interval: 5       # seconds between polls
  overlap: 60       # seconds read again before the cursor
  deletes: 300      # seconds between the id comparisons that find deletes (false: never)
  # batch: 500      # records per API call
  odoo:
    url: https://example.odoo.com
    db: example
    login: erpdoo-reader
    api_key: ${ERPDOO_POLL_API_KEY}
```

## The two modes

| | Replication (default) | Polling |
|---|---|---|
| Reads | Postgres' WAL, over a replication slot | Odoo's JSON-RPC API, as a read-only Odoo user |
| Needs | `wal_level=logical`, a role with `REPLICATION` and `SELECT`, an admin's SQL once | Odoo's external API and an API key; no database access |
| Latency | well under a second after the commit | the poll interval (5 s by default), plus the poll |
| Transitions | every transaction, exact | exact for fields Odoo tracks (from its chatter tracking); other fields compared between polls, so states between two polls are lost, and events are marked `approximate` |
| Records in events | as of the commit | as of the poll; tracked fields as of the change |
| Deletes | exact, when committed | found by comparing ids every `deletes` seconds, with the values of the last poll |
| `occurred_at` | commit time | Odoo's time of the change, to the second |
| After a stop | the slot holds every change until erpdoo is back | reads everything written since its cursor: tracked transitions all, others collapsed into one |
| Load | on Postgres (WAL decoding); none on Odoo | API calls on Odoo every poll, logged |
| `erpdoo backfill` | yes | not needed; not available (it reads Postgres) |

Events from the two modes have the same types, schemas and record shape,
so consumers need no change. Their `source` differs:

```json
"source": {"table": "account_move", "polling": true, "approximate": false, "mail_message_id": 105186}
```

`approximate: false` means the transition comes from Odoo's tracking
(`mail_message_id`); `true` means it was found by comparing values between
polls (`write_date` says which write), or it is a delete.

## API access

- **Replication mode** reads Postgres only. It uses Odoo's API only for
  inbound rules, as the integration user (see [Inbound](inbound.md)).
- **Polling mode** needs Odoo's external API (JSON-RPC with an API key).
  Self-hosted Odoo has it. On **Odoo Online it is only included in the
  Custom plan**; the Standard plan has no external API access, so neither
  polling mode nor inbound rules work there.
- erpdoo is developed and tested against Odoo 18 Community; Odoo Online and
  Odoo.sh run Enterprise, which erpdoo uses only through the same standard
  API but does not test.

## The read-only user

The user erpdoo polls as needs to read the rule models and their related
models, Odoo's model list (`ir.model`, `ir.model.fields`, which Odoo shows
only to administrators by default), its external ids (`ir.model.data`, to
tell when an app installed later was installed; administrators only by
default) and Odoo's field tracking (`mail.tracking.value`, administrators
only by default). In developer mode:

1. **Settings → Users & Companies → Groups → New**, e.g. `erpdoo: read only`,
   and under **Access Rights** add one line per model with only **Read**
   checked. For the shipped rules: `sale.order`, `sale.order.line`,
   `account.tax`, `account.move`, `account.move.line`, `stock.picking`,
   `stock.move`, `stock.picking.type`, `crm.lead`, `crm.stage`, `ir.model`,
   `ir.model.fields`, `ir.model.data` and `mail.tracking.value` (partners,
   products, currencies and languages every internal user may read).
2. **Settings → Users & Companies → Users → New**: login `erpdoo-reader`,
   type **Internal User**, no application rights, plus that group. It must
   see every company whose records the rules cover.
3. Give it an API key as for the integration user (see
   [The integration user](inbound.md#the-integration-user)).

[docker/odoo/init.py](../docker/odoo/init.py) does the same for the dev stack.

- At startup erpdoo checks the user cannot write, create or delete any
  model it reads, and logs a warning naming those it can. Every internal
  user can post chatter messages, which erpdoo never does.
- Without read access to `mail.tracking.value`, erpdoo still runs, but
  every rule compares values between polls (all events approximate), and
  it says so in its log.
- Without `ir.model.data`, an app installed later gets a baseline when
  erpdoo finds it, so changes made before give no event, and erpdoo says
  so too.

## How it works

Every interval, for each rule's model, erpdoo reads the records whose
`write_date` is at or after its cursor (the newest `write_date` seen) minus
`overlap`. Odoo sets `write_date` when a transaction *starts*, so a long
transaction can commit after the cursor has passed it; reading the overlap
again finds it, as long as it took less than `overlap` seconds (Odoo stops
requests after `limit_time_real`, 120 s by default, in worker mode).

- **Fields Odoo tracks** (a sale order's, invoice's or delivery's `state`,
  an invoice's `payment_state`): each tracking message is one write, with
  its exact old and new values, so every transition is seen, even several
  between two polls (confirmed and cancelled at once gives both events).
  The messages handled are noted, so reading the overlap again sends
  nothing twice.
- **Other fields** (`crm.lead`'s `probability`, which `LeadWon` uses):
  erpdoo compares each record with the values it stored at the previous
  poll. A lead won and lost again between two polls gives no event.
- **The first poll** of a model stores its records' current values as a
  baseline, as replication mode copies the rule tables; earlier changes
  give no event. A restart resumes from the cursor. A model created after
  erpdoo started polling (an app installed later) gets no baseline: its
  first poll reads every change since the model was created, as if erpdoo
  had polled it all along, and the events of changes from before erpdoo
  noticed the app (it checks every minute) say `"backfill": true`.
- **Event ids** come from the rule, the record and the tracking message
  (the id `erpdoo backfill` gives the same change) or the `write_date`.
  A poll is noted together with its events, which are then delivered and
  forgotten, so erpdoo stopping in between loses none: with Kafka
  transactions each is delivered once (see [Delivery](sinks.md#delivery)),
  otherwise one may be sent again with the same id, as in replication mode.
- **Related records** (customer, lines, products, taxes) are read for each
  poll in batches: one call per model and level of includes, whatever the
  number of events.

## Load on Odoo

Each poll costs one `search_read` per rule model and one for tracking
messages: 5 calls for the shipped rules when nothing changed, 60 a minute
at the default interval, plus 2 a minute that notice module installs. A
poll that finds changes reads the related records too (6 to 17 calls in the
polling test). The first poll reads every record of the rule models, a page
of 500 per call (48,000 sale orders took 99 calls and 7 s on the dev
stack). Every poll that found something logs its calls, and a summary is
logged every minute:

```
poll 7: 9 records read, 3 changed, 3 events, 14 API calls in 0.3 s
polling every 5 s: 13 polls in the last 60 s, 75 API calls (5.8 per poll, 75 per minute), 130 records read, 2 events
```

The [live viewer](architecture.md#live-viewer) shows each poll that found
something like a transaction, with its calls, and the load in its status
line. Odoo has no index on `write_date`, so on large tables each poll also
costs Postgres a scan; on hosted Odoo you cannot add one. A longer
`interval` lowers the load and raises the latency.

## Limits

- **States between two polls are lost** for fields Odoo does not track,
  and such events are `approximate`: their `from` is the value at the
  previous poll. Make the fields your rules depend on tracked if you can.
- **Untracked fields in an event are as of the poll**, not the change: an
  invoice posted and paid between two polls gives an `InvoicePosted` whose
  `amount_residual` is already 0 (its `payment_state`, tracked, is right).
- **Deletes are found late and approximately**: by the id comparison,
  every `deletes` seconds, dated when found, with the last polled values.
  A record created and deleted between two polls is never seen, and a
  record the user can no longer read (another company) looks deleted. The
  comparison reads every id of the model, so it only runs for models with
  delete rules.
- **Changes made without the ORM** (SQL, some imports) are seen only if
  they set `write_date`, and give no tracking.
- **Times are to the second**, Odoo's time of the change (`write_date` is
  the start of its transaction), not the commit.
- **Latency is the poll interval**, and each poll costs API calls (see
  [Load on Odoo](#load-on-odoo)).
- **Inbound loop marker**: an event caused by an inbound message is marked
  as in replication mode, but matched by the integration user, the record
  the message acted on and the time to the second (keep erpdoo's and
  Odoo's clocks in sync, within 2 s). If someone else writes the record
  before the poll, the marker is lost.
- **Odoo's API returns `0` and `false` for empty numbers and booleans**,
  where replication mode has `null`.

[scripts/polling_test.py](../scripts/polling_test.py) compares polling
mode's events with replication mode's on the dev stack and prints the
expected differences (see [CONTRIBUTING.md](../CONTRIBUTING.md#the-checks)).

## Trying it on the dev stack

The dev stack has an `erpdoo-reader` user. Set `source:` in
[erpdoo.yml](../erpdoo.yml) as above with `url: http://odoo:8069`,
`db: odoo`, and `docker compose restart erpdoo`.
