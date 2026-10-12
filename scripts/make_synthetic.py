#!/usr/bin/env python3
"""Generate a FAKE CSV shaped like the Kaggle e-commerce sales dataset.

Purpose: build/test the pipeline before (or if) the real Kaggle download is sorted out.
Never present this as the real dataset.

    python scripts/make_synthetic.py --out data/raw/synthetic_ecommerce.csv
"""
import argparse

import numpy as np
import pandas as pd

CATS = {  # base return rate, relative volume, price scale
    "Fashion": (0.12, 0.22, 45), "Electronics": (0.09, 0.18, 220), "Home": (0.06, 0.18, 60),
    "Beauty": (0.05, 0.12, 25), "Sports": (0.05, 0.12, 70), "Books": (0.03, 0.10, 18),
    "Grocery": (0.015, 0.08, 12),
}
REGIONS = ["North", "South", "East", "West", "Central"]
PAY = ["Credit Card", "Debit Card", "UPI", "Cash On Delivery", "Net Banking"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw/synthetic_ecommerce.csv")
    ap.add_argument("--rows", type=int, default=34500)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    rng = np.random.default_rng(a.seed)

    names = list(CATS)
    probs = np.array([CATS[c][1] for c in names]); probs = probs / probs.sum()
    cat = rng.choice(names, size=a.rows, p=probs)
    days = rng.integers(0, 730, size=a.rows)
    dates = pd.Timestamp("2023-01-01") + pd.to_timedelta(days, unit="D")

    base = np.array([CATS[c][0] for c in cat])
    # make it interesting: Fashion drifts up in the last 6 months, Electronics spikes in a few weeks
    late = days > 550
    base = np.where((cat == "Fashion") & late, base + 0.06, base)
    spike = (cat == "Electronics") & (days > 300) & (days < 340)
    base = np.where(spike, base + 0.08, base)
    returned = rng.random(a.rows) < base

    price = np.round(rng.gamma(2.0, [CATS[c][2] / 2 for c in cat]) + 3, 2)
    df = pd.DataFrame({
        "Order_ID": [f"ORD{100000 + i}" for i in range(a.rows)],
        "Customer_ID": rng.integers(1000, 9000, a.rows),
        "Product_ID": [f"P{rng.integers(1, 2500)}" for _ in range(a.rows)],
        "Category": cat,
        "Price": price,
        "Discount": np.round(rng.choice([0, 0.05, 0.1, 0.2], a.rows), 2),
        "Quantity": rng.integers(1, 5, a.rows),
        "Payment_Method": rng.choice(PAY, a.rows),
        "Order_Date": dates.strftime("%Y-%m-%d"),
        "Region": rng.choice(REGIONS, a.rows),
        "Return_Status": np.where(returned, "Returned", "Not Returned"),
    })
    df.to_csv(a.out, index=False)
    print(f"wrote {len(df):,} rows -> {a.out}  (SYNTHETIC)")


if __name__ == "__main__":
    main()
