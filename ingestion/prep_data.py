#!/usr/bin/env python3
"""Clean the raw Kaggle CSV into the schema the pipeline expects.

    python ingestion/prep_data.py --input data/raw/<kaggle_file>.csv

Column names differ between Kaggle datasets, so we match them case/underscore-insensitively
against alias lists. If a required column cannot be found the script tells you exactly which.

Output columns:
    order_id, product_id, category, region, payment_method, quantity, price, order_date, returned
"""
import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ALIASES = {
    "order_id": ["orderid", "ordernumber", "transactionid", "invoiceno", "invoice"],
    "product_id": ["productid", "sku", "stockcode", "itemid"],
    "category": ["category", "productcategory", "categoryname", "department"],
    "region": ["region", "country", "location", "state", "city"],
    "payment_method": ["paymentmethod", "payment", "paymenttype"],
    "quantity": ["quantity", "qty", "unitssold", "quantityordered"],
    "price": ["price", "unitprice", "productprice", "sellingprice"],
    "order_date": ["orderdate", "date", "purchasedate", "invoicedate", "ordertimestamp"],
    "returned": ["returnstatus", "returned", "isreturned", "returnflag", "return", "orderstatus", "status"],
}
REQUIRED = ["order_id", "category", "order_date", "price", "returned"]
TRUTHY = {"returned", "return", "yes", "y", "true", "1", "1.0", "refunded", "refund"}

DEFAULT_SYNTH_RATES = {  # only used with --synthesize-returns
    "Fashion": 0.15, "Electronics": 0.11, "Home": 0.07, "Beauty": 0.06,
    "Sports": 0.05, "Books": 0.03, "Grocery": 0.02,
}


def norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def find_columns(df: pd.DataFrame) -> dict:
    normalized = {norm(c): c for c in df.columns}
    found = {}
    for target, options in ALIASES.items():
        for opt in options:
            if opt in normalized:
                found[target] = normalized[opt]
                break
    return found


def to_flag(series: pd.Series) -> pd.Series:
    """'Returned'/'Yes'/1 -> 1 ; 'Not Returned'/'No'/0/NaN -> 0 (exact match, so 'Not Returned' stays 0)."""
    return series.astype(str).str.strip().str.lower().isin(TRUTHY).astype(int)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", default="data/clean/transactions_clean.csv")
    ap.add_argument("--sample-rows", type=int, default=1000, help="also write a small sample for local tests")
    ap.add_argument("--synthesize-returns", action="store_true",
                    help="use ONLY if the dataset has no return column; documents-in-report fallback")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    df = pd.read_csv(a.input)
    print(f"loaded {len(df):,} rows, columns: {list(df.columns)}")
    cols = find_columns(df)

    missing = [c for c in REQUIRED if c not in cols]
    if a.synthesize_returns and "returned" in missing:
        missing.remove("returned")
    if missing:
        sys.exit(f"ERROR: could not find columns for {missing}.\n"
                 f"Detected mapping: {cols}\nEdit ALIASES in prep_data.py or rename the CSV headers.")

    out = pd.DataFrame()
    out["order_id"] = df[cols["order_id"]].astype(str)
    out["product_id"] = df[cols["product_id"]].astype(str) if "product_id" in cols else out["order_id"]
    out["category"] = df[cols["category"]].astype(str).str.strip().str.title()
    out["region"] = df[cols["region"]].astype(str).str.strip().str.title() if "region" in cols else "Unknown"
    out["payment_method"] = (df[cols["payment_method"]].astype(str).str.strip().str.title()
                             if "payment_method" in cols else "Unknown")
    out["quantity"] = pd.to_numeric(df[cols["quantity"]], errors="coerce").fillna(1) if "quantity" in cols else 1
    out["price"] = pd.to_numeric(df[cols["price"]], errors="coerce")
    out["order_date"] = pd.to_datetime(df[cols["order_date"]], errors="coerce", dayfirst=False)

    if "returned" in cols:
        out["returned"] = to_flag(df[cols["returned"]])
    else:
        rng = np.random.default_rng(a.seed)
        p = out["category"].map(DEFAULT_SYNTH_RATES).fillna(0.06)
        out["returned"] = (rng.random(len(out)) < p).astype(int)
        print("WARNING: return flag was SYNTHESIZED. Say so in the report.")

    before = len(out)
    out = out.dropna(subset=["order_date", "price", "category"])
    out = out[(out["quantity"] > 0) & (out["price"] > 0)]
    out = out.drop_duplicates(subset=["order_id"])
    out["quantity"] = out["quantity"].astype(int)
    out["order_date"] = out["order_date"].dt.strftime("%Y-%m-%d")
    out = out.sort_values(["order_date", "order_id"]).reset_index(drop=True)

    Path(a.output).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(a.output, index=False)
    out.head(a.sample_rows).to_csv(Path(a.output).with_name("sample.csv"), index=False)

    # profile (paste this into the report's dataset section)
    print(f"\nkept {len(out):,} of {before:,} rows -> {a.output}")
    print(f"date range : {out['order_date'].min()} -> {out['order_date'].max()}")
    print(f"categories : {out['category'].nunique()}  | overall return rate: {out['returned'].mean():.2%}")
    prof = (out.groupby("category")["returned"].agg(orders="size", returns="sum", rate="mean")
            .sort_values("rate", ascending=False))
    prof["rate"] = (prof["rate"] * 100).round(2)
    print(prof.to_string())
    over = (prof["rate"] > 10).sum()
    print(f"\ncategories above 10% overall: {over}")
    if over == 0:
        print("NOTE: no category exceeds 10% overall - weekly cells may still cross it; check the weekly view.")


if __name__ == "__main__":
    main()
