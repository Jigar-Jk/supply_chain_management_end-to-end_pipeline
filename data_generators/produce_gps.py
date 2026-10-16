"""
produce_gps.py – GPS/RFID telemetry producer for the streaming ingestion pattern.

Sends JSON events for the trucks carrying in-transit shipments (data/shipments/shipments.jsonl)
to a Kafka topic. Locally the topic lives on the Kafka broker in local/streaming/docker-compose.yml;
in the cloud the same code targets Azure Event Hubs through its Kafka endpoint (set
--bootstrap to <namespace>.servicebus.windows.net:9093 and EVENTHUB_CONNECTION_STRING).

Event (one JSON object per message):
    event_id, event_type (GPS | RFID), truck_id, pallet_id (RFID only), shipment_id,
    lat, lon, speed_kmh, ts (ISO 8601 UTC event time)

Trucks move along their shipment's real route (origin warehouse -> destination). About
DARK_TRUCK_RATE of trucks are "dark": their tracker reports with a 3-hour lag, so their
latest ping is stale and the delay-risk GPS term flags them.

Bad events (~2.5%, logged in data/_manifests/gps_events.json):
    lat_out_of_range (lat=999)     -> quarantined in Silver (RULE_GPS_COORDINATES)
    missing_truck_id               -> quarantined in Silver (RULE_GPS_TRUCK_ID_MISSING)
    unparseable                    -> dead-letter table at Bronze (never reaches Bronze proper)
    duplicate                      -> same message sent twice; Bronze dedups on event_id

Usage:
    python -m data_generators.produce_gps --events 20000 --eps 1000          # local Kafka
    python -m data_generators.produce_gps --sink file --events 5000          # no broker needed
"""

import argparse
import hashlib
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_TOPIC = "gps-events"
DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_SEED = 42

DARK_TRUCK_RATE = 0.08
DARK_LAG = timedelta(hours=3)
RFID_RATE = 0.2
FAILURE_RATES = {           # per event; ~2.5% in total
    "lat_out_of_range": 0.010,
    "missing_truck_id": 0.010,
    "unparseable": 0.0025,
    "duplicate": 0.0025,
}


# ---------------------------------------------------------------------------
# Fleet: in-transit shipments with their route geometry
# ---------------------------------------------------------------------------

def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Generate master data, orders and shipments first.")
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def build_fleet(shipments: list[dict], routes: list[dict], damaged_shipment_ids: set, seed: int) -> list[dict]:
    """One truck per clean in-transit shipment, positioned somewhere along its route."""
    route_geo = {r["route_id"]: r for r in routes if r.get("route_id")}
    rng = random.Random(f"{seed}:fleet")
    fleet, seen = [], set()
    for s in shipments:
        sid = s.get("shipment_id")
        if (not sid or sid in seen or sid in damaged_shipment_ids
                or s.get("shipment_status") != "IN_TRANSIT" or s.get("route_id") not in route_geo):
            continue
        seen.add(sid)
        r = route_geo[s["route_id"]]
        fleet.append({
            "shipment_id": sid,
            "truck_id": "TRK-" + hashlib.sha256(f"{seed}:{sid}".encode()).hexdigest()[:8].upper(),
            "pallets": [f"PLT-{hashlib.sha256(f'{sid}:{i}'.encode()).hexdigest()[:8].upper()}"
                        for i in range(rng.randint(1, 4))],
            "origin": (r["origin_latitude"], r["origin_longitude"]),
            "destination": (r["destination_latitude"], r["destination_longitude"]),
            "progress": rng.uniform(0.05, 0.9),
            "dark": rng.random() < DARK_TRUCK_RATE,
        })
    if not fleet:
        raise ValueError("No clean in-transit shipments to track.")
    return fleet


def load_fleet(data_dir: Path, seed: int = DEFAULT_SEED) -> list[dict]:
    manifest_file = data_dir / "_manifests" / "shipments.json"
    damaged = set()
    if manifest_file.exists():
        damaged = {m["shipment_id"] for m in json.loads(manifest_file.read_text(encoding="utf-8"))["corrupted_records"]
                   if m["failure_mode"] != "duplicate" and m.get("shipment_id")}
    return build_fleet(_read_jsonl(data_dir / "shipments" / "shipments.jsonl"),
                       _read_jsonl(data_dir / "routes" / "routes_synthetic.jsonl"), damaged, seed)


# ---------------------------------------------------------------------------
# Event generation (pure: clock and RNG injected, so tests are deterministic)
# ---------------------------------------------------------------------------

def _clean_event(truck: dict, rng: random.Random, now: datetime) -> dict:
    truck["progress"] = min(0.99, truck["progress"] + rng.uniform(0.0005, 0.003))
    p = truck["progress"]
    (lat0, lon0), (lat1, lon1) = truck["origin"], truck["destination"]
    ts = now - DARK_LAG if truck["dark"] else now
    rfid = rng.random() < RFID_RATE
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
        "event_type": "RFID" if rfid else "GPS",
        "truck_id": truck["truck_id"],
        "pallet_id": rng.choice(truck["pallets"]) if rfid else None,
        "shipment_id": truck["shipment_id"],
        "lat": round(lat0 + (lat1 - lat0) * p + rng.uniform(-0.002, 0.002), 5),
        "lon": round(lon0 + (lon1 - lon0) * p + rng.uniform(-0.002, 0.002), 5),
        "speed_kmh": round(max(0.0, rng.gauss(72, 12)), 1),
        "ts": ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z",
    }


def generate_messages(
    fleet: list[dict],
    count: int,
    seed: int = DEFAULT_SEED,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    manifest: list | None = None,
) -> Iterator[str]:
    """Yield `count` logical events as JSON strings (a duplicate yields its message twice)."""
    rng = random.Random(f"{seed}:gps")
    bad_rng = random.Random(f"{seed}:gps:bad")
    thresholds, acc = [], 0.0
    for mode, rate in FAILURE_RATES.items():
        acc += rate
        thresholds.append((acc, mode))

    for i in range(count):
        event = _clean_event(rng.choice(fleet), rng, clock())
        roll = bad_rng.random()
        mode = next((m for t, m in thresholds if roll < t), None)
        if mode is None:
            yield json.dumps(event)
            continue
        if manifest is not None:
            manifest.append({"failure_mode": mode, "event_id": event["event_id"], "index": i})
        if mode == "lat_out_of_range":
            event["lat"] = 999.0
            yield json.dumps(event)
        elif mode == "missing_truck_id":
            event.pop("truck_id")
            yield json.dumps(event)
        elif mode == "unparseable":
            yield "{not-json " + event["event_id"]
        else:  # duplicate: the same message delivered twice (at-least-once producer retry)
            message = json.dumps(event)
            yield message
            yield message


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------

def kafka_config(bootstrap: str) -> dict:
    """Producer config. Uses SASL_SSL with the Event Hubs connection string when it is set."""
    config = {"bootstrap.servers": bootstrap, "linger.ms": 20, "acks": "all"}
    conn = os.environ.get("EVENTHUB_CONNECTION_STRING")
    if conn:
        config.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": conn,
        })
    return config


class KafkaSink:
    def __init__(self, bootstrap: str, topic: str):
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError("pip install confluent-kafka  (see requirements-streaming.txt)") from exc
        self.topic = topic
        self.producer = Producer(kafka_config(bootstrap))
        self.failed = 0

    def _on_delivery(self, err, _msg):
        if err is not None:
            self.failed += 1

    def send(self, key: str | None, message: str) -> None:
        while True:
            try:
                self.producer.produce(self.topic, value=message.encode("utf-8"),
                                      key=key.encode("utf-8") if key else None, on_delivery=self._on_delivery)
                break
            except BufferError:          # local queue full: back-pressure
                self.producer.poll(0.5)
        self.producer.poll(0)

    def close(self) -> None:
        remaining = self.producer.flush(30)
        if remaining or self.failed:
            raise RuntimeError(f"{self.failed} message(s) failed, {remaining} undelivered")


class FileSink:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(path, "w", encoding="utf-8")

    def send(self, key: str | None, message: str) -> None:
        self.f.write(message + "\n")

    def close(self) -> None:
        self.f.close()


def _message_key(message: str) -> str | None:
    """Partition by truck so each truck's events stay ordered within a partition."""
    try:
        return json.loads(message).get("truck_id")
    except ValueError:
        return None


def run(sink, fleet: list[dict], count: int, eps: float, seed: int = DEFAULT_SEED,
        data_dir: Path = DEFAULT_DATA_DIR) -> int:
    manifest: list = []
    sent, start = 0, time.monotonic()
    for i, message in enumerate(generate_messages(fleet, count, seed, manifest=manifest)):
        sink.send(_message_key(message), message)
        sent += 1
        if eps and i % 100 == 99:   # pace to the requested events/second
            ahead = sent / eps - (time.monotonic() - start)
            if ahead > 0:
                time.sleep(ahead)
    sink.close()
    elapsed = time.monotonic() - start
    manifest_file = data_dir / "_manifests" / "gps_events.json"
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    manifest_file.write_text(json.dumps({
        "seed": seed, "logical_events": count, "messages_sent": sent, "trucks": len(fleet),
        "dark_trucks": sum(t["dark"] for t in fleet), "corrupted_events": manifest,
    }, indent=2), encoding="utf-8")
    print(f"Sent {sent} messages ({count} events, {len(manifest)} corrupted) for {len(fleet)} trucks "
          f"in {elapsed:.1f}s ({sent / max(elapsed, 1e-9):.0f}/s)")
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce GPS/RFID telemetry to Kafka / Event Hubs.")
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--eps", type=float, default=1000, help="Target events per second (0 = unthrottled)")
    parser.add_argument("--sink", choices=["kafka", "file"], default="kafka")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--out", type=Path, default=DEFAULT_DATA_DIR / "gps" / "gps_events.jsonl",
                        help="Output file for --sink file")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    args = parser.parse_args()
    try:
        fleet = load_fleet(args.data_dir, args.seed)
        sink = KafkaSink(args.bootstrap, args.topic) if args.sink == "kafka" else FileSink(args.out)
        run(sink, fleet, args.events, args.eps, args.seed, args.data_dir)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
