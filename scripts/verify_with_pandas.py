#!/usr/bin/env python3
"""Independent check: recompute weekly return rates with pandas and compare with the MapReduce output.

    python scripts/verify_with_pandas.py --clean data/clean/transactions_clean.csv \
                                         --mr output/week_category.tsv
Exit code 0 = identical. Screenshot the PASS line for the report.
"""
import argparse
import sys

import pandas as pd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean", default="data/clean/transactions_clean.csv")
    ap.add_argument("--mr", default="output/week_category.tsv")
    a = ap.parse_args()

    df = pd.read_csv(a.clean, parse_dates=["order_date"])
    iso = df["order_date"].dt.isocalendar()
    df["week"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
    exp = df.groupby(["week", "category"]).agg(orders=("returned", "size"), returns=("returned", "sum")).reset_index()

    mr = pd.read_csv(a.mr, sep="\t")
    m = exp.merge(mr[["week", "category", "orders", "returns"]], on=["week", "category"],
                  how="outer", suffixes=("_pd", "_mr"), indicator=True)
    bad = m[(m["_merge"] != "both") | (m["orders_pd"] != m["orders_mr"]) | (m["returns_pd"] != m["returns_mr"])]
    if bad.empty:
        print(f"PASS: all {len(m)} (week, category) groups match between pandas and MapReduce")
        return 0
    print(f"FAIL: {len(bad)} mismatching groups\n", bad.head(20).to_string())
    return 1


if __name__ == "__main__":
    sys.exit(main())
