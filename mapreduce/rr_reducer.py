#!/usr/bin/env python3
"""Hadoop Streaming reducer / combiner for return-rate jobs.

--combine  : partial sums only, same output format as the mapper (used as the COMBINER).
default    : final output  <key parts...> <TAB> orders returns return_rate_pct order_value returned_value flag

Summing is associative + commutative, so using the same logic as a combiner is safe and
cuts the data shuffled from mappers to reducers.

flag = HIGH when return_rate_pct > --threshold AND orders >= --min-orders
       (min-orders stops a handful of orders in a quiet week from producing a fake 100% spike).
"""
import argparse
import sys


def emit(key, o, r, ov, rv, a):
    if a.combine:
        print(f"{key}\t{o},{r},{ov:.2f},{rv:.2f}")
        return
    rate = 100.0 * r / o if o else 0.0
    flag = "HIGH" if (rate > a.threshold and o >= a.min_orders) else "OK"
    print("\t".join(key.split("|") + [str(o), str(r), f"{rate:.2f}", f"{ov:.2f}", f"{rv:.2f}", flag]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--combine", action="store_true")
    ap.add_argument("--threshold", type=float, default=10.0)
    ap.add_argument("--min-orders", type=int, default=10)
    a = ap.parse_args()

    cur, o, r, ov, rv = None, 0, 0, 0.0, 0.0
    for line in sys.stdin:
        try:
            key, val = line.rstrip("\n").split("\t")
            vo, vr, vov, vrv = val.split(",")
            vo, vr, vov, vrv = int(vo), int(vr), float(vov), float(vrv)
        except ValueError:
            sys.stderr.write("reporter:counter:returns_job,bad_reducer_input,1\n")
            continue
        if key != cur:
            if cur is not None:
                emit(cur, o, r, ov, rv, a)
            cur, o, r, ov, rv = key, 0, 0, 0.0, 0.0
        o += vo
        r += vr
        ov += vov
        rv += vrv
    if cur is not None:
        emit(cur, o, r, ov, rv, a)


if __name__ == "__main__":
    main()
