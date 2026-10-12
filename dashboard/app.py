"""Return-rate dashboard (Streamlit + Plotly).   streamlit run dashboard/app.py

Reads the TSVs exported from HDFS into ./output. If they do not exist yet it falls back to
dashboard/sample_output/ (clearly marked) so the UI can be built before the pipeline works.
"""
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
REAL, SAMPLE = ROOT / "output", Path(__file__).resolve().parent / "sample_output"

st.set_page_config(page_title="Return-Rate Monitor", layout="wide")
src = REAL if (REAL / "week_category.tsv").exists() else SAMPLE
if src is SAMPLE:
    st.warning("Showing SAMPLE data (pipeline output not found in ./output).")


@st.cache_data
def load(folder: Path):
    wc = pd.read_csv(folder / "week_category.tsv", sep="\t")
    cr = pd.read_csv(folder / "category_region.tsv", sep="\t")
    # first day of ISO week for a proper time axis
    wc["week_start"] = pd.to_datetime(wc["week"] + "-1", format="%G-W%V-%u")
    return wc.sort_values("week_start"), cr


wc, cr = load(src)
st.title("Product Return-Rate Pattern Analysis")
st.caption("Weekly return rate per category - computed by MapReduce over events streamed via Kafka into HDFS")

with st.sidebar:
    threshold = st.slider("Alert threshold (%)", 1.0, 25.0, 10.0, 0.5)
    min_orders = st.slider("Min orders per week (noise guard)", 1, 100, 10)
    cats = st.multiselect("Categories", sorted(wc["category"].unique()), default=sorted(wc["category"].unique()))
    dmin, dmax = wc["week_start"].min().date(), wc["week_start"].max().date()
    rng = st.date_input("Period", (dmin, dmax), min_value=dmin, max_value=dmax)

f = wc[wc["category"].isin(cats)]
if len(rng) == 2:
    f = f[(f["week_start"].dt.date >= rng[0]) & (f["week_start"].dt.date <= rng[1])]
f = f.assign(flag_live=(f["return_rate_pct"] > threshold) & (f["orders"] >= min_orders))

tot_o, tot_r = f["orders"].sum(), f["returns"].sum()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Orders", f"{tot_o:,}")
c2.metric("Returns", f"{tot_r:,}")
c3.metric("Overall return rate", f"{100 * tot_r / max(tot_o, 1):.2f}%")
c4.metric("Returned value", f"{f['returned_value'].sum():,.0f}")

st.subheader("Weekly return rate by category")
fig = px.line(f, x="week_start", y="return_rate_pct", color="category",
              labels={"week_start": "Week", "return_rate_pct": "Return rate (%)"})
fig.add_hline(y=threshold, line_dash="dash", line_color="red", annotation_text=f"{threshold}% threshold")
st.plotly_chart(fig, use_container_width=True)

left, right = st.columns(2)
with left:
    st.subheader("Overall rate per category")
    agg = f.groupby("category").agg(orders=("orders", "sum"), returns=("returns", "sum")).reset_index()
    agg["rate"] = 100 * agg["returns"] / agg["orders"]
    bar = px.bar(agg.sort_values("rate", ascending=False), x="category", y="rate",
                 color=(agg["rate"] > threshold).map({True: "above threshold", False: "ok"}),
                 color_discrete_map={"above threshold": "#d62728", "ok": "#4c78a8"},
                 labels={"rate": "Return rate (%)", "color": ""})
    bar.add_hline(y=threshold, line_dash="dash", line_color="red")
    st.plotly_chart(bar, use_container_width=True)
with right:
    st.subheader("Category x region hotspots")
    pv = cr.pivot(index="category", columns="region", values="return_rate_pct")
    st.plotly_chart(px.imshow(pv, text_auto=".1f", aspect="auto", color_continuous_scale="Reds",
                              labels={"color": "Return rate (%)"}), use_container_width=True)

st.subheader(f"Flagged category-weeks (> {threshold}% with at least {min_orders} orders)")
flagged = f[f["flag_live"]].sort_values("return_rate_pct", ascending=False)
st.caption(f"{len(flagged)} of {len(f)} category-weeks flagged")
st.dataframe(flagged[["week", "category", "orders", "returns", "return_rate_pct", "returned_value"]],
             use_container_width=True, hide_index=True)
