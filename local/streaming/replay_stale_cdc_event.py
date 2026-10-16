"""
replay_stale_cdc_event.py – Failure demo: deliver an OLD CDC event again, after newer ones.

Out-of-order delivery happens in transport (retries, replays, a misbehaving consumer), never in
SQL Server's log. This tool reproduces it: it reads the order_lines CDC topic, finds an order line
whose state changed at least twice, and re-publishes its OLDER update with the same key, so the
topic now holds a stale event after the newer one.

Silver keeps the highest source_position per key, so after streaming, the order line must still
show its NEWER state, and reconcile_cdc.py must still PASS.

Usage (Kafka running locally):
    python -m local.streaming.replay_stale_cdc_event
"""

import json
import sys

from confluent_kafka import Consumer, Producer, TopicPartition

TOPIC = "scm.scm_oms.dbo.order_lines"
BOOTSTRAP = "localhost:9092"


def read_topic():
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "replay-stale-demo",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    consumer.assign([TopicPartition(TOPIC, p, 0) for p in partitions])
    messages, idle = [], 0
    while idle < 5:
        msg = consumer.poll(1.0)
        if msg is None:
            idle += 1
            continue
        idle = 0
        if msg.error() is None and msg.value():
            messages.append((msg.key(), json.loads(msg.value())))
    consumer.close()
    return messages


def main() -> None:
    try:
        updates = {}
        for key, event in read_topic():
            if event.get("op") == "u":
                updates.setdefault(key, []).append(event)
        candidates = [(k, evs) for k, evs in updates.items() if len(evs) >= 2]
        if not candidates:
            raise RuntimeError("No order line with 2+ updates yet. Run simulate_oms_changes first.")
        key, events = sorted(candidates, key=lambda kv: kv[0])[0]
        events.sort(key=lambda e: (e["source"]["commit_lsn"], e["source"]["change_lsn"] or ""))
        stale, newest = events[0], events[-1]

        producer = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})
        producer.produce(TOPIC, key=key, value=json.dumps(stale).encode("utf-8"))
        if producer.flush(10):
            raise RuntimeError("Replay message was not delivered")

        print(f"Re-published a STALE event for key {key.decode()}")
        print(f"  stale  : status {stale['after']['order_status']:<10} commit_lsn {stale['source']['commit_lsn']}")
        print(f"  newest : status {newest['after']['order_status']:<10} commit_lsn {newest['source']['commit_lsn']}")
        print("Expected after streaming: Silver keeps the NEWEST status; reconcile_cdc.py still PASSES.")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
