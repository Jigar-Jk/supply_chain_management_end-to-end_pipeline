"""
test_simulate_oms_changes.py – The OMS change simulator, against a fake SQL Server connection.

The fake connection answers the two state queries the simulator runs at start (order lines and
inventory) and records every other statement, so the simulator's logic can be checked without a
database: state pools, event mix, transactions, unique order IDs across runs, and bad changes.

Run locally with:
    python -m unittest tests.test_simulate_oms_changes -v
"""

import unittest
from collections import Counter
from decimal import Decimal

from data_generators.simulate_oms_changes import BAD_MODES, EVENT_WEIGHTS, Pool, Simulator


def seed_rows():
    orders, stock = [], []
    for i in range(60):
        status = ("CREATED", "PICKED", "SHIPPED", "DELIVERED")[i % 4]
        warehouse = f"WH-000{i % 3 + 1}"
        for line in (1, 2):
            product = f"SKU-{(i * 2 + line) % 20:06d}"
            orders.append((f"ORD-{i:04d}", line, status, 10, 10 if status in ("SHIPPED", "DELIVERED") else 0,
                           product, warehouse, f"CUST-{i % 7:06d}", Decimal("12.50")))
    for w in (1, 2, 3):
        for p in range(20):
            stock.append((f"WH-000{w}", f"SKU-{p:06d}", 40, 5, 15))
    return orders, stock


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, *params):
        if sql.startswith("SELECT order_id"):
            return iter(self.conn.orders)
        if sql.startswith("SELECT warehouse_id"):
            return iter(self.conn.stock)
        self.conn.statements.append((sql, params))
        return self


class FakeConnection:
    def __init__(self):
        self.orders, self.stock = seed_rows()
        self.statements, self.commits, self.rollbacks = [], 0, 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class TestPool(unittest.TestCase):

    def test_add_remove_and_choice(self):
        import random
        pool = Pool(["a", "b", "c"])
        pool.add("b")                          # no duplicates
        self.assertEqual(len(pool), 3)
        pool.remove("a")
        pool.remove("zzz")                     # removing a missing item is a no-op
        self.assertEqual(sorted(pool.items), ["b", "c"])
        self.assertIn(pool.choice(random.Random(1)), {"b", "c"})
        self.assertIsNone(Pool().choice(random.Random(1)))


class TestSimulator(unittest.TestCase):

    def run_steps(self, n=300, seed=42):
        conn = FakeConnection()
        sim = Simulator(conn, seed=seed)
        events = [sim.step() for _ in range(n)]
        return conn, sim, events

    def test_state_is_loaded_into_pools(self):
        conn = FakeConnection()
        sim = Simulator(conn)
        self.assertEqual(len(sim.state.lines), 60)
        self.assertEqual({s: len(p) for s, p in sim.state.by_status.items()},
                         {"CREATED": 15, "PICKED": 15, "SHIPPED": 15})
        self.assertEqual(len(sim.state.stock), 60)

    def test_every_event_is_one_committed_transaction(self):
        conn, _, events = self.run_steps(200)
        self.assertEqual(conn.commits, 200)
        self.assertEqual(conn.rollbacks, 0)
        self.assertTrue(set(Counter(e for e, _ in events)) <= set(EVENT_WEIGHTS))

    def test_deterministic_choices_for_same_seed_and_state(self):
        _, _, a = self.run_steps(150)
        _, _, b = self.run_steps(150)
        self.assertEqual([e for e, _ in a], [e for e, _ in b])

    def test_status_moves_through_the_lifecycle(self):
        conn = FakeConnection()
        sim = Simulator(conn)
        order = sim.state.by_status["CREATED"].items[0]
        sim.state.by_status["CREATED"] = Pool([order])
        sim.pick_order()
        self.assertIn(order, sim.state.by_status["PICKED"].index)
        sql, params = conn.statements[-1]
        self.assertIn("UPDATE dbo.order_lines SET order_status", sql)
        self.assertEqual(params[:2], ("PICKED", order))

    def test_ship_draws_down_stock_in_the_same_transaction(self):
        conn = FakeConnection()
        sim = Simulator(conn)
        order = sim.state.by_status["PICKED"].items[0]
        sim.state.by_status["PICKED"] = Pool([order])
        before = len(conn.statements)
        sim.ship_order()
        sqls = [s for s, _ in conn.statements[before:]]
        self.assertTrue(any("UPDATE dbo.order_lines" in s for s in sqls))
        self.assertTrue(any("UPDATE dbo.inventory" in s for s in sqls))
        self.assertIn(order, sim.state.by_status["SHIPPED"].index)

    def test_new_order_ids_are_unique_across_runs(self):
        _, sim_a, events_a = self.run_steps(200)
        _, sim_b, events_b = self.run_steps(200)
        ids_a = {d["order_id"] for e, d in events_a if e == "new_order"}
        ids_b = {d["order_id"] for e, d in events_b if e == "new_order"}
        self.assertTrue(ids_a and ids_b)
        self.assertNotEqual(sim_a.run_id, sim_b.run_id)
        self.assertFalse(ids_a & ids_b, "a second run must not reuse order IDs (primary key violation)")

    def test_bad_changes_write_the_intended_bad_values(self):
        conn = FakeConnection()
        sim = Simulator(conn)
        seen = set()
        for _ in range(200):
            before = len(conn.statements)
            details = sim.bad_change()
            if details:
                seen.add(details["bad_mode"])
                sql, params = conn.statements[before]
                if details["bad_mode"] == "negative_quantity":
                    self.assertIn("ordered_quantity = -5", sql)
                elif details["bad_mode"] == "invalid_status":
                    self.assertEqual(params[0], "SHIPPPED")
                elif details["bad_mode"] == "negative_on_hand":
                    self.assertIn("quantity_on_hand = -3", sql)
            sim.conn.commit()
        self.assertEqual(seen, set(BAD_MODES))

    def test_remove_line_issues_a_delete(self):
        conn = FakeConnection()
        sim = Simulator(conn)
        details = sim.remove_line()
        self.assertIsNotNone(details)
        self.assertTrue(conn.statements[-1][0].startswith("DELETE FROM dbo.order_lines"))
        self.assertNotIn(details["order_line_number"], sim.state.lines[details["order_id"]])


if __name__ == "__main__":
    unittest.main()
