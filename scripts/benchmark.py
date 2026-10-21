"""Measure erpdoo's end-to-end latency and throughput on the dev stack.

Latency is from the Postgres commit (the event's occurred_at, the commit
timestamp) to the moment this script's Kafka consumer receives the event.
All containers share the host's clock, so the two times are comparable.

Three phases, all with SaleOrderConfirmed:
  1. latency:  confirm orders one at a time, waiting for each event
  2. burst:    confirm many orders concurrently while erpdoo runs; the rate
               is limited by whichever of Odoo and erpdoo is slower
  3. backlog:  stop erpdoo, confirm the same number of orders, start erpdoo
               and time how fast it works through the backlog (erpdoo and
               Kafka only, Odoo is out of the picture)

    python scripts/benchmark.py [--sequential 200] [--burst 3000]

It creates thousands of orders in the dev database, and phase 3 stops and
starts the erpdoo container with docker compose.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition

from smoke_test import Odoo, wait_until


class Collector:
    """Receives SaleOrderConfirmed events in a background thread."""

    def __init__(self, bootstrap: str, topic: str) -> None:
        self.consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"bench-{uuid.uuid4()}",
                                  "enable.auto.commit": False})
        partitions = self.consumer.list_topics(topic, timeout=10).topics[topic].partitions
        self.consumer.assign([TopicPartition(topic, p, self.consumer.get_watermark_offsets(
            TopicPartition(topic, p), timeout=10)[1]) for p in partitions])
        self.received: dict[int, list[tuple[float, float]]] = {}  # record id -> [(commit, received)]
        self.lock = threading.Lock()
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        while not self.stopped.is_set():
            msg = self.consumer.poll(0.05)
            if msg is None or msg.error():
                continue
            now = time.time()
            value = msg.value()
            # Skip the schema registry's header (zero byte, schema id) of JSON Schema messages.
            event = json.loads(value[5:] if value[:1] == b"\0" else value)
            if event["event_type"] == "SaleOrderConfirmed":
                commit = datetime.fromisoformat(event["occurred_at"]).timestamp()
                with self.lock:
                    self.received.setdefault(event["record_id"], []).append((commit, now))

    def wait_for(self, ids: list[int], timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self.lock:
                if all(i in self.received for i in ids):
                    return True
            time.sleep(0.05)
        return False

    def timings(self, ids: list[int]) -> list[tuple[float, float]]:
        with self.lock:
            return [self.received[i][0] for i in ids if i in self.received]

    def duplicates(self, ids: list[int]) -> int:
        with self.lock:
            return sum(len(self.received.get(i, [])) - 1 for i in ids if i in self.received)

    def stop(self) -> None:
        self.stopped.set()
        self.thread.join()


def percentiles(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)

    def pick(p):
        return ordered[min(len(ordered) - 1, round(p / 100 * (len(ordered) - 1)))]

    return {"p50": pick(50), "p95": pick(95), "p99": pick(99), "max": ordered[-1],
            "mean": statistics.fmean(ordered)}


def create_quotations(odoo: Odoo, partners: list[int], product: int, count: int) -> list[int]:
    """Quotations in chunks; partner i % len(partners) so concurrent batches
    touch different customers."""
    ids: list[int] = []
    for start in range(0, count, 250):
        vals = [{"partner_id": partners[(start + i) % len(partners)],
                 "order_line": [(0, 0, {"product_id": product, "product_uom_qty": 1})]}
                for i in range(min(250, count - start))]
        ids.extend(odoo("sale.order", "create", vals))
    return ids


def confirm_concurrently(odoo: Odoo, ids: list[int], batch: int, workers: int) -> tuple[float, int]:
    """One action_confirm call (one transaction) per batch, ``workers`` at a
    time. Returns the wall time and the number of batches that failed."""
    batches = [ids[i:i + batch] for i in range(0, len(ids), batch)]
    failed = 0

    def confirm(chunk):
        nonlocal failed
        for attempt in range(3):
            try:
                odoo("sale.order", "action_confirm", chunk)
                return
            except Exception:
                time.sleep(0.5 * (attempt + 1))
        failed += 1

    started = time.monotonic()
    with ThreadPoolExecutor(workers) as pool:
        list(pool.map(confirm, batches))
    return time.monotonic() - started, failed


def compose(*args: str) -> str:
    return subprocess.run(["docker", "compose", *args], check=True, capture_output=True, text=True).stdout


def streaming_since(started: float, timeout: float = 60) -> float:
    """When erpdoo logged that it is streaming, after ``started`` (epoch)."""
    since = datetime.fromtimestamp(started, timezone.utc).isoformat()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for line in compose("logs", "erpdoo", "--timestamps", "--no-log-prefix", "--since", since).splitlines():
            if "streaming to" in line:
                stamp = line.split()[0]  # RFC 3339 with nanoseconds
                return datetime.fromisoformat(stamp[:26] + "+00:00").timestamp()
        time.sleep(0.2)
    raise SystemExit("erpdoo did not start streaming")


def machine() -> dict[str, str]:
    def first(path, key):
        try:
            for line in open(path):
                if line.startswith(key):
                    return line.split(":", 1)[1].strip() if ":" in line else line.split("=", 1)[1].strip().strip('"')
        except OSError:
            pass
        return "unknown"

    mem_kb = first("/proc/meminfo", "MemTotal").split()[0]
    docker = subprocess.run(["docker", "version", "--format", "{{.Server.Version}}"],
                            capture_output=True, text=True).stdout.strip()
    return {
        "cpu": first("/proc/cpuinfo", "model name"),
        "logical_cpus": str(os.cpu_count()),
        "memory": f"{int(mem_kb) / 1024 / 1024:.1f} GiB" if mem_kb.isdigit() else "unknown",
        "os": first("/etc/os-release", "PRETTY_NAME="),
        "kernel": platform.release(),
        "docker": docker or "unknown",
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--odoo", default="http://localhost:8069")
    p.add_argument("--db", default="odoo")
    p.add_argument("--kafka", default="127.0.0.1:19092")
    p.add_argument("--topic", default="erpdoo.events")
    p.add_argument("--sequential", type=int, default=200, help="orders for the latency phase")
    p.add_argument("--burst", type=int, default=3000, help="orders for each of the burst and backlog phases")
    p.add_argument("--batch", type=int, default=25, help="orders confirmed per transaction in a burst")
    p.add_argument("--workers", type=int, default=8, help="concurrent confirm calls in a burst")
    p.add_argument("--json", help="also write the results to this file")
    args = p.parse_args()

    wait_until("Odoo", lambda: Odoo(args.odoo, args.db, "admin", "admin"))
    odoo = Odoo(args.odoo, args.db, "admin", "admin")
    partners = odoo("res.partner", "search", [("customer_rank", ">", 0)], limit=max(args.workers * 2, 16))
    product = odoo("product.product", "search", [("sale_ok", "=", True), ("type", "=", "service")], limit=1)[0]
    results: dict = {"machine": machine(), "settings": vars(args)}

    collector = Collector(args.kafka, args.topic)

    # 1. Latency, one order at a time.
    ids = create_quotations(odoo, partners, product, args.sequential)
    print(f"latency: confirming {len(ids)} orders one at a time")
    for oid in ids:
        odoo("sale.order", "action_confirm", [oid])
        if not collector.wait_for([oid], 30):
            print(f"  no event for order {oid} within 30s")
    lat = [(r - c) * 1000 for c, r in collector.timings(ids)]
    results["latency_ms"] = percentiles(lat)
    print("  ms:", {k: round(v, 1) for k, v in results["latency_ms"].items()})

    # 2. Burst while erpdoo runs.
    ids = create_quotations(odoo, partners, product, args.burst)
    print(f"burst: confirming {len(ids)} orders, {args.batch} per transaction, {args.workers} at a time")
    odoo_seconds, failed = confirm_concurrently(odoo, ids, args.batch, args.workers)
    complete = collector.wait_for(ids, 300)
    t = collector.timings(ids)
    commits, received = [c for c, _ in t], [r for _, r in t]
    span = max(received) - min(commits)
    results["burst"] = {
        "orders": len(ids), "events": len(t), "duplicates": collector.duplicates(ids), "failed_batches": failed,
        "complete": complete, "odoo_seconds": odoo_seconds,
        "commit_span_s": max(commits) - min(commits), "delivery_span_s": span,
        "events_per_second": len(t) / span, "lag_after_last_commit_s": max(received) - max(commits),
        "latency_ms": percentiles([(r - c) * 1000 for c, r in t]),
    }
    print("  ", {k: (round(v, 2) if isinstance(v, float) else v) for k, v in results["burst"].items() if k != "latency_ms"})

    # 3. Backlog: erpdoo stopped while Odoo commits, then started.
    ids = create_quotations(odoo, partners, product, args.burst)
    print(f"backlog: stopping erpdoo, confirming {len(ids)} orders, starting erpdoo")
    compose("stop", "erpdoo")
    confirm_concurrently(odoo, ids, args.batch, args.workers)
    started = time.time()
    # --no-deps: "start" would also re-run the one-shot odoo-init service.
    compose("up", "-d", "--no-deps", "erpdoo")
    streaming = streaming_since(started)
    complete = collector.wait_for(ids, 600)
    t = collector.timings(ids)
    received = [r for _, r in t]
    drain = max(received) - streaming
    results["backlog"] = {
        "orders": len(ids), "events": len(t), "duplicates": collector.duplicates(ids), "complete": complete,
        "startup_s": streaming - started,  # container start to "streaming to"
        "drain_s": drain,  # "streaming to" to the last event received
        "events_per_second": len(t) / drain,
    }
    print("  ", {k: (round(v, 2) if isinstance(v, float) else v) for k, v in results["backlog"].items()})
    collector.stop()

    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, indent=2)
    ok = results["burst"]["complete"] and results["backlog"]["complete"] and not results["burst"]["failed_batches"]
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
