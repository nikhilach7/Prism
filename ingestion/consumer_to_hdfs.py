#!/usr/bin/env python3
"""Kafka -> HDFS writer (micro-batching).

Reads events from the topic, buffers them per ORDER MONTH, and every N events / N seconds writes
one JSON-lines file per month into  /data/ecom/raw/month=YYYY-MM/batch-<ts>-<n>.jsonl .
Offsets are committed only after the files are safely written (at-least-once); duplicate
event_ids are dropped inside the consumer's memory.

Why partition by month (not week)? Weekly folders would create hundreds of tiny files
(the HDFS "small files" problem). Weekly return rate is computed later, inside MapReduce.

    python ingestion/consumer_to_hdfs.py --sink hdfs --idle-exit 20          # real run
    python ingestion/consumer_to_hdfs.py --source file:events.jsonl --sink local   # test, no Kafka/HDFS

--sink hdfs  streams each file through `docker exec -i namenode hdfs dfs -put - <path>`
             (avoids the host<->datanode hostname problem that WebHDFS has with Docker).
--sink local writes under data/hdfs_local/ with the same folder layout.
"""
import argparse
import json
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import config  # noqa: E402


class HdfsSink:
    def put(self, rel_path: str, payload: str):
        path = f"{config.HDFS_RAW_DIR}/{rel_path}"
        parent = path.rsplit("/", 1)[0]
        subprocess.run(["docker", "exec", config.NAMENODE_CONTAINER, "hdfs", "dfs", "-mkdir", "-p", parent],
                       check=True, capture_output=True)
        subprocess.run(["docker", "exec", "-i", config.NAMENODE_CONTAINER, "hdfs", "dfs", "-put", "-f", "-", path],
                       input=payload.encode(), check=True, capture_output=True)


class LocalSink:
    def __init__(self, root="data/hdfs_local/raw"):
        self.root = Path(root)

    def put(self, rel_path: str, payload: str):
        p = self.root / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(payload)


def kafka_source(idle_exit: int):
    """Yield (event_dict, flush_callback) style: yields events; yields None on idle poll."""
    from confluent_kafka import Consumer

    c = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest", "enable.auto.commit": False,
    })
    c.subscribe([config.TOPIC])
    last_msg = time.time()
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                if idle_exit and time.time() - last_msg > idle_exit:
                    return
                yield None, c
                continue
            if msg.error():
                print("kafka error:", msg.error(), file=sys.stderr)
                continue
            last_msg = time.time()
            yield parse(msg.value()), c
    finally:
        c.close()


def parse(raw):
    """Return the event dict, or None if the record is malformed (skipped, never crashes the consumer)."""
    try:
        e = json.loads(raw)
        e["event_id"], e["order_date"][:7]  # required fields present?
        return e
    except (ValueError, KeyError, TypeError):
        print("skipping malformed record", file=sys.stderr)
        return None


def file_source(path: str):
    with open(path) as f:
        for line in f:
            yield parse(line), None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="kafka", help="'kafka' or 'file:<events.jsonl>'")
    ap.add_argument("--sink", choices=["hdfs", "local"], default="hdfs")
    ap.add_argument("--idle-exit", type=int, default=20, help="stop after N idle seconds (0 = run forever)")
    a = ap.parse_args()

    sink = HdfsSink() if a.sink == "hdfs" else LocalSink()
    src = file_source(a.source[5:]) if a.source.startswith("file:") else kafka_source(a.idle_exit)

    buffers = defaultdict(list)
    seen = set()
    n_buf = n_written = n_dupes = n_files = 0
    last_flush = time.time()
    consumer = None

    def flush():
        nonlocal n_buf, n_written, n_files, last_flush
        if not n_buf:
            last_flush = time.time()
            return
        ts = int(time.time() * 1000)
        for month, events in buffers.items():
            n_files += 1
            sink.put(f"month={month}/batch-{ts}-{n_files}.jsonl", "\n".join(json.dumps(e) for e in events) + "\n")
        n_written += n_buf
        buffers.clear()
        n_buf = 0
        last_flush = time.time()
        if consumer is not None:
            consumer.commit(asynchronous=False)  # commit only after data is stored
        print(f"flushed -> total written {n_written:,} events in {n_files} files")

    for event, c in src:
        consumer = c or consumer
        if event is not None:
            if event["event_id"] in seen:
                n_dupes += 1
            else:
                seen.add(event["event_id"])
                buffers[event["order_date"][:7]].append(event)
                n_buf += 1
        if n_buf >= config.BATCH_MAX_EVENTS or (n_buf and time.time() - last_flush >= config.BATCH_MAX_SECONDS):
            flush()
    flush()
    print(f"finished: {n_written:,} events written, {n_dupes} duplicates dropped, {n_files} files")


if __name__ == "__main__":
    main()
