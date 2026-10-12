#!/usr/bin/env python3
"""Replay the cleaned CSV as a stream of events into Kafka.

Every order becomes an `order` event. Every returned order ALSO becomes a `return_request` event,
dated a few days after the order (deterministic per order, so re-runs give identical data).

    python ingestion/producer.py --rate 500                 # live replay into Kafka
    python ingestion/producer.py --dry-run events.jsonl     # no Kafka: write the events to a file

event_id is a uuid5 of (order_id, event_type) -> re-sending the same data never creates new ids,
which lets the consumer drop duplicates (at-least-once delivery).
"""
import argparse
import hashlib
import json
import sys
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import config  # noqa: E402

NS = uuid.UUID("12345678-1234-5678-1234-567812345678")


def return_delay_days(order_id: str) -> int:
    """Stable pseudo-random 2..21 day delay derived from the order id."""
    h = int(hashlib.md5(order_id.encode()).hexdigest(), 16)
    return 2 + h % 20


def build_events(df: pd.DataFrame):
    events = []
    for r in df.itertuples(index=False):
        base = {
            "order_id": r.order_id, "product_id": r.product_id, "category": r.category,
            "region": r.region, "payment_method": r.payment_method,
            "quantity": int(r.quantity), "price": float(r.price), "order_date": r.order_date,
        }
        events.append({**base, "event_id": str(uuid.uuid5(NS, f"{r.order_id}:order")),
                       "event_type": "order", "event_date": r.order_date})
        if int(r.returned) == 1:
            d = date.fromisoformat(r.order_date) + timedelta(days=return_delay_days(r.order_id))
            events.append({**base, "event_id": str(uuid.uuid5(NS, f"{r.order_id}:return_request")),
                           "event_type": "return_request", "event_date": d.isoformat()})
    events.sort(key=lambda e: (e["event_date"], e["order_id"], e["event_type"]))
    return events


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=config.CLEAN_CSV)
    ap.add_argument("--rate", type=float, default=500, help="events per second (0 = as fast as possible)")
    ap.add_argument("--limit", type=int, default=0, help="only send the first N events (testing)")
    ap.add_argument("--dry-run", metavar="FILE", help="write events to FILE instead of Kafka")
    a = ap.parse_args()

    df = pd.read_csv(a.input, dtype={"order_id": str, "product_id": str})
    events = build_events(df)
    if a.limit:
        events = events[: a.limit]
    n_ret = sum(e["event_type"] == "return_request" for e in events)
    print(f"{len(events):,} events ({len(events) - n_ret:,} orders, {n_ret:,} return requests)")

    if a.dry_run:
        with open(a.dry_run, "w") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")
        print(f"dry run: wrote {a.dry_run}")
        return

    from confluent_kafka import Producer  # imported here so --dry-run works without it

    errors = []
    p = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 20, "acks": "all"})

    def on_delivery(err, _msg):
        if err:
            errors.append(str(err))

    delay = 1.0 / a.rate if a.rate > 0 else 0
    t0 = time.time()
    for i, e in enumerate(events, 1):
        p.produce(config.TOPIC, key=e["order_id"], value=json.dumps(e), on_delivery=on_delivery)
        p.poll(0)
        if delay:
            time.sleep(delay)
        if i % 5000 == 0:
            print(f"  sent {i:,}/{len(events):,}")
    p.flush()
    print(f"done in {time.time() - t0:.1f}s, delivery errors: {len(errors)}")
    if errors:
        print("first error:", errors[0])
        sys.exit(1)


if __name__ == "__main__":
    main()
