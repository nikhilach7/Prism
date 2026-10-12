#!/usr/bin/env python3
"""Hadoop Streaming mapper for return-rate jobs.

Input : JSON-lines events (one per line) from HDFS
Output: key <TAB> orders,returns,order_value,returned_value

    --group week_category   key = ISO-week|category        (Job 1: weekly return rate per category)
    --group category_region key = category|region          (Job 2: where are returns concentrated)

An `order` event contributes (1,0,price*qty,0); a `return_request` contributes (0,1,0,price*qty).
Both are keyed by the ORDER date, so a return is counted against the week the order was placed
(cohort view) -> the rate can never exceed 100%.

Test locally without Hadoop:
    cat events.jsonl | python3 rr_mapper.py --group week_category | sort | python3 rr_reducer.py
"""
import argparse
import json
import sys
from datetime import date


def iso_week(d: str) -> str:
    y, w, _ = date.fromisoformat(d).isocalendar()
    return f"{y}-W{w:02d}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", choices=["week_category", "category_region"], default="week_category")
    a = ap.parse_args()

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            e = json.loads(line)
            value = float(e["price"]) * int(e["quantity"])
            if a.group == "week_category":
                key = f"{iso_week(e['order_date'])}|{e['category']}"
            else:
                key = f"{e['category']}|{e.get('region', 'Unknown')}"
            if e["event_type"] == "order":
                out = f"1,0,{value:.2f},0"
            elif e["event_type"] == "return_request":
                out = f"0,1,0,{value:.2f}"
            else:
                raise ValueError("unknown event_type")
        except (ValueError, KeyError, TypeError):
            # shows up under Job counters in the YARN UI -> good evidence for the report
            sys.stderr.write("reporter:counter:returns_job,bad_records,1\n")
            continue
        print(f"{key}\t{out}")


if __name__ == "__main__":
    main()
