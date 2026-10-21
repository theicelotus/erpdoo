"""Live event viewer: a small web page that shows each transaction's raw
changes next to the events erpdoo built from it, as they are delivered.

Enabled with ``viewer:`` in erpdoo.yml. It has no authentication and shows
business data, so bind it to localhost or a private network only.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse

from erpdoo.events import Event
from erpdoo.pgoutput import format_lsn
from erpdoo.replication import Change, Transaction

log = logging.getLogger(__name__)

MAX_CHANGES = 50  # per transaction; a module install writes thousands of rows
HISTORY = 200  # transactions a newly opened page starts with
MAX_VALUES = 8  # fields shown for an inserted row
NOISE = {"create_uid", "create_date", "write_uid", "write_date"}


def describe_change(change: Change) -> dict:
    """A change for display. For updates, the fields that changed; the old
    values come from Postgres or, for tables in the state store, from the
    store (StateStore.apply_transaction)."""
    row = change.new or change.old or {}
    item = {"op": change.op, "table": change.table, "model": change.model, "id": row.get("id")}
    if change.op == "update":
        before = change.old if change.old is not None and not change.old_is_key_only else None
        item["changed"] = None if before is None else {
            # The store keeps only the columns rules use; others are not compared.
            k: [before[k], v] for k, v in change.new.items() if k not in NOISE and k in before and before[k] != v
        }
        # Odoo rewrites rows for bookkeeping (write_date, computed fields that
        # kept their value); the page hides these unless asked.
        item["internal"] = item["changed"] == {}
    elif change.op == "insert":
        values = {k: v for k, v in change.new.items() if v is not None and k not in NOISE and k != "id"}
        item["values"] = dict(list(values.items())[:MAX_VALUES])
        item["more"] = max(0, len(values) - MAX_VALUES)
    return item


def describe(txn: Transaction, events: list[Event]) -> dict:
    return {"xid": txn.xid, "commit_lsn": format_lsn(txn.commit_lsn), "commit_time": txn.commit_time.isoformat(),
            **describe_changes(txn.changes, events)}


def describe_poll(number: int, at: datetime, changes: list[Change], events: list[Event], calls: int) -> dict:
    """A poll (polling mode) for display, like a transaction: the records it
    found changed (only the fields rules use) next to their events."""
    return {"label": f"poll {number}", "commit_time": at.isoformat(), "detail": f"{calls} API calls",
            **describe_changes(changes, events)}


def describe_changes(all_changes: list[Change], events: list[Event]) -> dict:
    changes = [describe_change(c) for c in all_changes[:MAX_CHANGES]]
    # Link the field changes that made a rule fire to the event they produced:
    # the event's transition fields, on the event's record.
    for event in events:
        for item in changes:
            if item["model"] != event.model or item["id"] != event.record_id:
                continue
            shown = item.get("changed") or item.get("values") or {}
            for name in event.payload.get("changes", {}):
                if name in shown:
                    item.setdefault("fires", {}).setdefault(name, []).append(event.event_id)
    return {
        "change_count": len(all_changes),
        "table_count": len({c.table for c in all_changes}),
        "changes": changes,
        "events": [e.payload for e in events],
    }


class Viewer:
    def __init__(self, host: str = "127.0.0.1", port: int = 8000) -> None:
        self.host, self.port = host, port
        self.history: deque[dict] = deque(maxlen=HISTORY)
        self.clients: set[asyncio.Queue] = set()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.server: uvicorn.Server | None = None
        self.slot: dict | None = None  # slot health (polling: poll status), set by erpdoo's main loop
        self.app = self._build_app()

    def _build_app(self) -> FastAPI:
        @asynccontextmanager
        async def lifespan(_app):
            self.loop = asyncio.get_running_loop()
            yield

        app = FastAPI(title="erpdoo viewer", lifespan=lifespan, docs_url=None, redoc_url=None)

        @app.get("/", response_class=HTMLResponse)
        async def page():
            return PAGE

        @app.get("/slot")
        async def slot():
            return self.slot

        @app.get("/recent")
        async def recent():
            return list(self.history)

        @app.get("/stream")
        async def stream(request: Request):
            queue: asyncio.Queue = asyncio.Queue(maxsize=100)
            self.clients.add(queue)

            async def messages():
                try:
                    while not await request.is_disconnected():
                        try:
                            item = await asyncio.wait_for(queue.get(), timeout=15)
                            yield f"data: {json.dumps(item, default=str)}\n\n"
                        except asyncio.TimeoutError:
                            yield ": keepalive\n\n"
                finally:
                    self.clients.discard(queue)

            return StreamingResponse(messages(), media_type="text/event-stream",
                                     headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

        return app

    def start(self) -> None:
        """Serve the page from a background thread."""
        self.server = uvicorn.Server(uvicorn.Config(self.app, host=self.host, port=self.port, log_level="warning"))
        threading.Thread(target=self.server.run, name="erpdoo-viewer", daemon=True).start()
        log.info("viewer on http://%s:%d", self.host, self.port)

    def publish(self, item: dict) -> None:
        """Called from erpdoo's main loop; hands the item to the server thread."""
        if self.loop is not None:
            self.loop.call_soon_threadsafe(self._broadcast, item)

    def _broadcast(self, item: dict) -> None:
        self.history.append(item)
        for queue in list(self.clients):
            try:
                queue.put_nowait(item)
            except asyncio.QueueFull:
                pass  # a page that stopped reading; it catches up on reload

    def stop(self) -> None:
        if self.server is not None:
            self.server.should_exit = True


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>erpdoo live</title>
<style>
  :root { --bg: #f6f7f9; --card: #fff; --text: #1d2330; --muted: #6b7385; --line: #e3e6eb;
          --ins: #1a7f37; --upd: #9a6700; --del: #cf222e; --event: #0b5cad; --event-bg: #eaf2fc; }
  @media (prefers-color-scheme: dark) {
    :root { --bg: #11151c; --card: #1a2029; --text: #e3e7ee; --muted: #8e97a8; --line: #2a313c;
            --ins: #4ac26b; --upd: #d4a72c; --del: #f47067; --event: #6cb6ff; --event-bg: #1b2a3d; }
  }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.45 system-ui, sans-serif; background: var(--bg); color: var(--text); }
  header { position: sticky; top: 0; background: var(--bg); padding: 12px 16px; border-bottom: 1px solid var(--line);
           display: flex; gap: 16px; align-items: center; flex-wrap: wrap; z-index: 1; }
  h1 { font-size: 16px; margin: 0; }
  .status { color: var(--muted); }
  .status.live::before { content: "\\25CF "; color: var(--ins); }
  .status.warn { color: var(--del); font-weight: 600; }
  label { color: var(--muted); user-select: none; }
  main { padding: 12px 16px; display: grid; gap: 10px; }
  .txn { background: var(--card); border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }
  .txn.has-events { border-color: var(--event); }
  .meta { padding: 6px 12px; color: var(--muted); font-size: 12px; border-bottom: 1px solid var(--line); }
  .cols { display: grid; grid-template-columns: 1fr 1fr; }
  .col { padding: 8px 12px; min-width: 0; }
  .col + .col { border-left: 1px solid var(--line); }
  .col h2 { font-size: 11px; text-transform: uppercase; letter-spacing: .05em; color: var(--muted); margin: 0 0 6px; }
  .change { margin: 0 0 6px; overflow-wrap: anywhere; }
  .op { font-weight: 600; font-size: 12px; }
  .op.insert { color: var(--ins); } .op.update { color: var(--upd); } .op.delete, .op.truncate { color: var(--del); }
  .fields { color: var(--muted); font-size: 12px; font-family: ui-monospace, monospace; }
  .summary { font-weight: 600; }
  .fire { color: var(--event); background: var(--event-bg); border-radius: 3px; padding: 0 3px; text-decoration: none; font-weight: 600; }
  .fire:hover { text-decoration: underline; }
  .change.internal .fields { font-style: italic; }
  body:not(.show-internal) .change.internal { display: none; }
  body.show-internal .internal-note { display: none; }
  .event { background: var(--event-bg); border-radius: 6px; padding: 6px 8px; margin: 0 0 6px; scroll-margin-top: 70px; }
  .event:target { outline: 2px solid var(--event); }
  .fired-by { color: var(--muted); font-size: 12px; }
  .event b { color: var(--event); }
  .event pre { margin: 6px 0 0; font-size: 12px; white-space: pre-wrap; overflow-wrap: anywhere; }
  .none { color: var(--muted); font-style: italic; }
  .empty { color: var(--muted); text-align: center; padding: 40px 0; }
  @media (max-width: 720px) { .cols { grid-template-columns: 1fr; } .col + .col { border-left: 0; border-top: 1px solid var(--line); } }
</style>
</head>
<body>
<header>
  <h1>erpdoo live</h1>
  <span id="status" class="status">connecting</span>
  <span id="slot" class="status" title="how far erpdoo's replication slot trails Postgres">slot: checking</span>
  <label><input type="checkbox" id="only-events"> only transactions with events</label>
  <label><input type="checkbox" id="show-internal"> show internal-only updates</label>
  <label><input type="checkbox" id="pause"> pause</label>
</header>
<main id="list"><p class="empty">Waiting for changes. Confirm a sale order in Odoo.</p></main>
<script>
const list = document.getElementById("list"), statusEl = document.getElementById("status");
const onlyEvents = document.getElementById("only-events"), pause = document.getElementById("pause");
const esc = s => String(s).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}[c]));
const short = v => { const s = typeof v === "object" && v !== null ? JSON.stringify(v) : String(v); return s.length > 60 ? s.slice(0, 57) + "..." : s; };
let queued = [];

// A field change that made a rule fire links to the event it produced.
function field(c, k, text) {
  const ids = (c.fires || {})[k];
  return ids ? `<a class="fire" href="#ev-${esc(ids[0])}" title="made a rule fire">${text}</a>` : text;
}

function changeHtml(c) {
  let detail = "";
  if (c.op === "update") {
    detail = c.changed === null ? "previous row not known"
      : c.internal ? "only internal fields changed"
      : Object.entries(c.changed).map(([k, [a, b]]) => field(c, k, `${esc(k)}: ${esc(short(a))} \\u2192 ${esc(short(b))}`)).join(", ");
  } else if (c.op === "insert") {
    detail = Object.entries(c.values).map(([k, v]) => field(c, k, `${esc(k)}=${esc(short(v))}`)).join(", ") + (c.more ? `, +${c.more} more` : "");
  }
  const what = c.model ? `${esc(c.model)} <span class="fields">(${esc(c.table)})</span>` : esc(c.table);
  return `<div class="change${c.internal ? " internal" : ""}"><span class="op ${c.op}">${c.op.toUpperCase()}</span> ${what}${c.id != null ? " #" + c.id : ""}`
    + (detail ? `<div class="fields">${detail}</div>` : "") + `</div>`;
}

const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;

function eventHtml(e, changes) {
  const by = changes.filter(c => Object.values(c.fires || {}).some(ids => ids.includes(e.event_id)))
    .map(c => `${esc(c.model)} #${c.id} ${Object.keys(c.fires).filter(k => c.fires[k].includes(e.event_id)).map(esc).join(", ")}`);
  return `<div class="event" id="ev-${esc(e.event_id)}"><b>${esc(e.event_type)}</b> ${esc(e.model)}:${e.record_id}`
    + (by.length ? `<div class="fired-by">fired by ${[...new Set(by)].join("; ")}</div>` : "")
    + `<details><summary>payload</summary><pre>${esc(JSON.stringify(e, null, 2))}</pre></details></div>`;
}

function render(t) {
  const el = document.createElement("section");
  el.className = "txn" + (t.events.length ? " has-events" : "");
  el.dataset.events = t.events.length;
  const hidden = t.change_count - t.changes.length;
  const internal = t.changes.filter(c => c.internal).length;
  el.innerHTML = `<div class="meta"><span class="summary">${plural(t.change_count, "change")} in ${plural(t.table_count, "table")} \\u2192 ${plural(t.events.length, "event")}</span>
    \\u00b7 ${t.label ? esc(t.label) : "transaction " + t.xid} \\u00b7 ${new Date(t.commit_time).toLocaleTimeString()}${t.commit_lsn ? " \\u00b7 LSN " + esc(t.commit_lsn) : ""}${t.detail ? " \\u00b7 " + esc(t.detail) : ""}</div>
    <div class="cols"><div class="col"><h2>Raw changes</h2>${t.changes.map(changeHtml).join("")}
      ${internal ? `<div class="none internal-note">+${plural(internal, "update")} with only internal fields changed</div>` : ""}
      ${hidden > 0 ? `<div class="none">+${hidden} more not shown</div>` : ""}</div>
    <div class="col"><h2>Events</h2>${t.events.length ? t.events.map(e => eventHtml(e, t.changes)).join("") : '<div class="none">no event</div>'}</div></div>`;
  el.hidden = onlyEvents.checked && !t.events.length;
  return el;
}

// The newest event's payload starts open; the one opened before it closes
// again unless the reader toggled it.
let autoOpened = null;
function openNewest(card) {
  const details = card.querySelector(".event details");
  if (!details) return;
  if (autoOpened && autoOpened.dataset.auto) autoOpened.open = false;
  details.open = true;
  details.dataset.auto = "1";
  details.querySelector("summary").addEventListener("click", () => delete details.dataset.auto);
  autoOpened = details;
}

function add(t) {
  if (pause.checked) { queued.push(t); return; }
  const empty = list.querySelector(".empty"); if (empty) empty.remove();
  const card = render(t);
  list.prepend(card);
  openNewest(card);
  while (list.children.length > 300) list.lastChild.remove();
}

const showInternal = document.getElementById("show-internal");
showInternal.onchange = () => document.body.classList.toggle("show-internal", showInternal.checked);

onlyEvents.onchange = () => { for (const el of list.querySelectorAll(".txn")) el.hidden = onlyEvents.checked && el.dataset.events === "0"; };
pause.onchange = () => { if (!pause.checked) { const q = queued; queued = []; q.forEach(add); } };

// Slot lag (polling mode: the polls' load), refreshed every 10 seconds
// (erpdoo measures it every minute).
const slotEl = document.getElementById("slot");
function showSlot() {
  fetch("slot").then(r => r.json()).then(h => {
    if (!h) return;
    slotEl.textContent = h.text.replace(/^slot \S+: /, "slot: ") + (h.warnings.length ? " \u2014 " + h.warnings.join("; ") : "");
    slotEl.className = "status" + (h.warnings.length ? " warn" : "");
    if (h.title) slotEl.title = h.title;
  }).catch(() => {});
}
showSlot();
setInterval(showSlot, 10000);

fetch("recent").then(r => r.json()).then(items => {
  items.forEach(add);
  const source = new EventSource("stream");
  source.onopen = () => { statusEl.textContent = "live"; statusEl.className = "status live"; };
  source.onerror = () => { statusEl.textContent = "reconnecting"; statusEl.className = "status"; };
  source.onmessage = m => add(JSON.parse(m.data));
});
</script>
</body>
</html>
"""
