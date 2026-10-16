"""
simulate_oms_changes.py – Live change traffic for the SQL Server OMS source (scm_oms), for CDC.

Acts like the order-management application: picks orders, ships them (and draws down stock),
delivers them, takes new orders (reserving stock), corrects quantities, cancels, removes lines
and receives replenishments. Every change is ordinary INSERT / UPDATE / DELETE SQL, so SQL
Server's CDC captures it exactly like real application traffic.

Each business event is ONE transaction. Shipping, for example, updates order_lines and
inventory together, so all its changes share one commit LSN in the transaction log, which
Debezium carries through as the ordering key.

About 2% of events are deliberately bad (negative quantity, misspelled status, over-shipment,
negative stock). They flow through CDC so the Silver quality gate has to quarantine them.

State is read from SQL Server once at start; the simulator is the only writer, so it keeps
that state in step as it goes. Same database state + same seed -> same run.
Manifest of every event: data/_manifests/oms_changes.json.

Usage:
    python -m data_generators.simulate_oms_changes --events 2000 --tps 20
"""

import argparse
import hashlib
import json
import random
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from data_generators.load_sqlserver_oms import connect

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_SEED = 42

EVENT_WEIGHTS = {
    "pick_order": 20,
    "ship_order": 20,
    "deliver_order": 15,
    "new_order": 15,
    "receive_stock": 12,
    "correct_quantity": 8,
    "cancel_order": 5,
    "remove_line": 3,
    "bad_change": 2,
}
BAD_MODES = ["negative_quantity", "invalid_status", "shipped_exceeds_ordered", "negative_on_hand"]
SHORT_SHIP_RATE = 0.03
NOW_SQL = "SYSUTCDATETIME()"


class Pool:
    """Ordered set with O(1) add/remove and deterministic random choice."""

    def __init__(self, items=()):
        self.items, self.index = [], {}
        for item in items:
            self.add(item)

    def add(self, item):
        if item not in self.index:
            self.index[item] = len(self.items)
            self.items.append(item)

    def remove(self, item):
        i = self.index.pop(item, None)
        if i is None:
            return
        last = self.items.pop()
        if i < len(self.items):
            self.items[i] = last
            self.index[last] = i

    def choice(self, rng):
        return rng.choice(self.items) if self.items else None

    def __len__(self):
        return len(self.items)


class OmsState:
    """In-memory mirror of the parts of scm_oms the simulator needs."""

    def __init__(self, conn):
        cur = conn.cursor()
        self.lines = {}                       # order_id -> {line_no: {...}}
        self.order_warehouse = {}
        self.by_status = {s: Pool() for s in ("CREATED", "PICKED", "SHIPPED")}
        self.prices = {}
        customers = set()
        for (order_id, line_no, status, ordered, shipped, product_id, warehouse_id, customer_id,
             price) in cur.execute(
                "SELECT order_id, order_line_number, order_status, ordered_quantity, shipped_quantity, "
                "product_id, warehouse_id, customer_id, unit_price FROM dbo.order_lines "
                "ORDER BY order_id, order_line_number"):
            self.lines.setdefault(order_id, {})[line_no] = {
                "status": status, "ordered": ordered, "shipped": shipped, "product_id": product_id}
            self.order_warehouse[order_id] = warehouse_id
            if customer_id and customer_id.startswith("CUST-") and customer_id != "CUST-999999":
                customers.add(customer_id)
            if product_id and price is not None and price > 0:
                self.prices.setdefault(product_id, float(price))
        for order_id, lines in self.lines.items():
            status = next(iter(lines.values()))["status"]
            if status in self.by_status:
                self.by_status[status].add(order_id)
        self.customers = sorted(customers)

        self.stock = {}                        # (warehouse_id, product_id) -> {"on_hand", "reserved", "reorder"}
        self.assortment = {}                   # warehouse_id -> [product_id] with a known price
        for warehouse_id, product_id, on_hand, reserved, reorder in cur.execute(
                "SELECT warehouse_id, product_id, quantity_on_hand, quantity_reserved, reorder_point "
                "FROM dbo.inventory ORDER BY warehouse_id, product_id"):
            self.stock[(warehouse_id, product_id)] = {
                "on_hand": on_hand or 0, "reserved": reserved or 0, "reorder": reorder or 0}
            if product_id in self.prices:
                self.assortment.setdefault(warehouse_id, []).append(product_id)
        self.low_stock = Pool(k for k, v in self.stock.items() if v["on_hand"] <= v["reorder"])
        self.warehouses = sorted(self.assortment)

    def set_status(self, order_id, old, new):
        if old in self.by_status:
            self.by_status[old].remove(order_id)
        if new in self.by_status:
            self.by_status[new].add(order_id)
        for line in self.lines[order_id].values():
            line["status"] = new


class Simulator:
    def __init__(self, conn, seed=DEFAULT_SEED):
        self.conn = conn
        self.cur = conn.cursor()
        self.state = OmsState(conn)
        self.rng = random.Random(f"{seed}:oms-changes")
        self.seed = seed
        self.new_orders = 0
        # New order IDs must be unique across runs (the counter restarts each run), so they also
        # hash this run's start time. Event choices stay deterministic for a given seed and state.
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")

    # ------------------------------------------------------------------ helpers
    def _reserve(self, warehouse_id, product_id, qty):
        """Adjust reserved stock (qty may be negative to release)."""
        stock = self.state.stock.get((warehouse_id, product_id))
        if stock is None or not qty:
            return
        stock["reserved"] = max(0, stock["reserved"] + qty)
        self.cur.execute(
            f"UPDATE dbo.inventory SET quantity_reserved = ?, updated_at = {NOW_SQL} "
            "WHERE warehouse_id = ? AND product_id = ?", stock["reserved"], warehouse_id, product_id)

    def _order_status_update(self, order_id, old, new):
        self.cur.execute(
            f"UPDATE dbo.order_lines SET order_status = ?, updated_at = {NOW_SQL} "
            "WHERE order_id = ? AND order_status = ?", new, order_id, old)
        self.state.set_status(order_id, old, new)

    # ------------------------------------------------------------------ business events
    def pick_order(self):
        order_id = self.state.by_status["CREATED"].choice(self.rng)
        if order_id is None:
            return None
        self._order_status_update(order_id, "CREATED", "PICKED")
        return {"order_id": order_id}

    def ship_order(self):
        order_id = self.state.by_status["PICKED"].choice(self.rng)
        if order_id is None:
            return None
        warehouse_id = self.state.order_warehouse[order_id]
        for line_no, line in self.state.lines[order_id].items():
            ordered = line["ordered"] or 0
            shipped = ordered if self.rng.random() > SHORT_SHIP_RATE else int(ordered * self.rng.uniform(0.5, 0.95))
            shipped = max(0, shipped)
            line["shipped"] = shipped
            self.cur.execute(
                f"UPDATE dbo.order_lines SET order_status = 'SHIPPED', shipped_quantity = ?, updated_at = {NOW_SQL} "
                "WHERE order_id = ? AND order_line_number = ?", shipped, order_id, line_no)
            stock = self.state.stock.get((warehouse_id, line["product_id"]))
            if stock is not None and shipped:
                stock["on_hand"] = max(0, stock["on_hand"] - shipped)
                stock["reserved"] = max(0, stock["reserved"] - max(0, ordered))
                self.cur.execute(
                    f"UPDATE dbo.inventory SET quantity_on_hand = ?, quantity_reserved = ?, updated_at = {NOW_SQL} "
                    "WHERE warehouse_id = ? AND product_id = ?",
                    stock["on_hand"], stock["reserved"], warehouse_id, line["product_id"])
                if stock["on_hand"] <= stock["reorder"]:
                    self.state.low_stock.add((warehouse_id, line["product_id"]))
        self.state.set_status(order_id, "PICKED", "SHIPPED")
        return {"order_id": order_id, "lines": len(self.state.lines[order_id])}

    def deliver_order(self):
        order_id = self.state.by_status["SHIPPED"].choice(self.rng)
        if order_id is None:
            return None
        self._order_status_update(order_id, "SHIPPED", "DELIVERED")
        return {"order_id": order_id}

    def new_order(self):
        if not self.state.warehouses or not self.state.customers:
            return None
        warehouse_id = self.rng.choice(self.state.warehouses)
        products = self.state.assortment[warehouse_id]
        self.new_orders += 1
        order_id = "ORD-" + hashlib.sha256(
            f"{self.seed}:sim:{self.run_id}:{self.new_orders}".encode()).hexdigest()[:12].upper()
        customer_id = self.rng.choice(self.state.customers)
        today = datetime.now(timezone.utc).date()
        promised = today + timedelta(days=self.rng.randint(2, 7))
        lines = {}
        for line_no, product_id in enumerate(self.rng.sample(products, min(len(products), self.rng.randint(1, 5))), 1):
            qty = self.rng.choice([1, 2, 3, 5, 10, 12, 20, 24, 50])
            price = round(self.state.prices[product_id] * self.rng.uniform(0.9, 1.0), 2)
            self.cur.execute(
                "INSERT INTO dbo.order_lines (order_id, order_line_number, customer_id, product_id, warehouse_id, "
                "order_date, promised_delivery_date, ordered_quantity, shipped_quantity, unit_price, order_status, "
                f"updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 'CREATED', {NOW_SQL})",
                order_id, line_no, customer_id, product_id, warehouse_id, today, promised, qty, price)
            lines[line_no] = {"status": "CREATED", "ordered": qty, "shipped": 0, "product_id": product_id}
            self._reserve(warehouse_id, product_id, qty)
        self.state.lines[order_id] = lines
        self.state.order_warehouse[order_id] = warehouse_id
        self.state.by_status["CREATED"].add(order_id)
        return {"order_id": order_id, "lines": len(lines)}

    def receive_stock(self):
        key = self.state.low_stock.choice(self.rng) or self.rng.choice(list(self.state.stock))
        stock = self.state.stock[key]
        received = max(1, stock["reorder"] * self.rng.randint(2, 4))
        stock["on_hand"] += received
        self.cur.execute(
            f"UPDATE dbo.inventory SET quantity_on_hand = ?, updated_at = {NOW_SQL} "
            "WHERE warehouse_id = ? AND product_id = ?", stock["on_hand"], *key)
        self.state.low_stock.remove(key)
        return {"warehouse_id": key[0], "product_id": key[1], "received": received}

    def correct_quantity(self):
        order_id = self.state.by_status["CREATED"].choice(self.rng)
        if order_id is None:
            return None
        line_no = self.rng.choice(sorted(self.state.lines[order_id]))
        line = self.state.lines[order_id][line_no]
        new_qty = max(1, (line["ordered"] or 1) + self.rng.choice([-2, -1, 1, 2, 5]))
        self._reserve(self.state.order_warehouse[order_id], line["product_id"], new_qty - (line["ordered"] or 0))
        line["ordered"] = new_qty
        self.cur.execute(
            f"UPDATE dbo.order_lines SET ordered_quantity = ?, updated_at = {NOW_SQL} "
            "WHERE order_id = ? AND order_line_number = ?", new_qty, order_id, line_no)
        return {"order_id": order_id, "order_line_number": line_no, "ordered_quantity": new_qty}

    def cancel_order(self):
        status = self.rng.choice(["CREATED", "PICKED"])
        order_id = self.state.by_status[status].choice(self.rng)
        if order_id is None:
            return None
        for line in self.state.lines[order_id].values():
            self._reserve(self.state.order_warehouse[order_id], line["product_id"], -(line["ordered"] or 0))
        self._order_status_update(order_id, status, "CANCELLED")
        return {"order_id": order_id, "from_status": status}

    def remove_line(self):
        candidates = [o for o in self.state.by_status["CREATED"].items[:500] if len(self.state.lines[o]) > 1]
        if not candidates:
            return None
        order_id = self.rng.choice(candidates)
        line_no = max(self.state.lines[order_id])
        line = self.state.lines[order_id].pop(line_no)
        self._reserve(self.state.order_warehouse[order_id], line["product_id"], -(line["ordered"] or 0))
        self.cur.execute("DELETE FROM dbo.order_lines WHERE order_id = ? AND order_line_number = ?", order_id, line_no)
        return {"order_id": order_id, "order_line_number": line_no}

    def bad_change(self):
        mode = self.rng.choice(BAD_MODES)
        if mode == "negative_quantity":
            order_id = self.state.by_status["CREATED"].choice(self.rng)
            if order_id is None:
                return None
            line_no = min(self.state.lines[order_id])
            self.state.lines[order_id][line_no]["ordered"] = -5
            self.cur.execute(
                f"UPDATE dbo.order_lines SET ordered_quantity = -5, updated_at = {NOW_SQL} "
                "WHERE order_id = ? AND order_line_number = ?", order_id, line_no)
            return {"bad_mode": mode, "order_id": order_id, "order_line_number": line_no}
        if mode == "invalid_status":
            order_id = self.state.by_status["PICKED"].choice(self.rng)
            if order_id is None:
                return None
            self._order_status_update(order_id, "PICKED", "SHIPPPED")   # typo on purpose; leaves the pools
            return {"bad_mode": mode, "order_id": order_id}
        if mode == "shipped_exceeds_ordered":
            order_id = self.state.by_status["SHIPPED"].choice(self.rng)
            if order_id is None:
                return None
            line_no = min(self.state.lines[order_id])
            line = self.state.lines[order_id][line_no]
            line["shipped"] = (line["ordered"] or 0) + 7
            self.cur.execute(
                f"UPDATE dbo.order_lines SET shipped_quantity = ?, updated_at = {NOW_SQL} "
                "WHERE order_id = ? AND order_line_number = ?", line["shipped"], order_id, line_no)
            return {"bad_mode": mode, "order_id": order_id, "order_line_number": line_no}
        key = self.rng.choice(list(self.state.stock))
        self.state.stock[key]["on_hand"] = -3
        self.cur.execute(
            f"UPDATE dbo.inventory SET quantity_on_hand = -3, updated_at = {NOW_SQL} "
            "WHERE warehouse_id = ? AND product_id = ?", *key)
        return {"bad_mode": mode, "warehouse_id": key[0], "product_id": key[1]}

    # ------------------------------------------------------------------ driver
    def step(self):
        """Run one business event as one transaction; returns (event, details) or None."""
        events, weights = zip(*EVENT_WEIGHTS.items())
        event = self.rng.choices(events, weights)[0]
        try:
            details = getattr(self, event)()
            if details is None:                       # nothing eligible: take a new order instead
                event, details = "new_order", self.new_order()
            self.conn.commit()
            return event, details
        except Exception:
            self.conn.rollback()
            raise


def run(server="localhost", database="scm_oms", events=2000, tps=20.0, seed=DEFAULT_SEED,
        data_dir=DEFAULT_DATA_DIR) -> Counter:
    conn = connect(server, database)
    sim = Simulator(conn, seed)
    print(f"Loaded state: {len(sim.state.lines)} orders "
          f"({ {s: len(p) for s, p in sim.state.by_status.items()} }), {len(sim.state.stock)} stock rows")
    counts, log, start = Counter(), [], time.monotonic()
    for i in range(events):
        event, details = sim.step()
        counts[event] += 1
        log.append({"seq": i, "event": event, **(details or {})})
        if tps:
            ahead = (i + 1) / tps - (time.monotonic() - start)
            if ahead > 0:
                time.sleep(ahead)
    conn.close()
    elapsed = time.monotonic() - start

    manifest_file = data_dir / "_manifests" / "oms_changes.json"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(json.dumps({
        "seed": seed, "run_id": sim.run_id, "events": events,
        "counts": dict(counts), "log": log}, indent=2, default=str), encoding="utf-8")
    print(f"Ran {events} business events in {elapsed:.1f}s ({events / max(elapsed, 1e-9):.1f}/s): {dict(counts)}")
    print(f"Manifest -> {manifest_file}")
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate live OMS changes in SQL Server for CDC.")
    parser.add_argument("--events", type=int, default=2000)
    parser.add_argument("--tps", type=float, default=20, help="Business events (transactions) per second; 0 = unthrottled")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--server", default="localhost")
    parser.add_argument("--database", default="scm_oms")
    args = parser.parse_args()
    try:
        run(args.server, args.database, args.events, args.tps, args.seed)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
