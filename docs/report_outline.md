# PRISM — Report Outline

*(BDA Lab Project #28: Product Return-Rate Pattern Analysis. Fill in each section as the pipeline
and demo come together — owner: D, everyone contributes their part.)*

## 1. Problem statement and objective
Finance teams cannot identify which products or categories are driving rising return costs because
return data is analyzed only in quarterly reviews. PRISM streams return-request events via Kafka,
stores them on HDFS, and runs a MapReduce job to compute weekly return rate by product category,
flagging any category-week exceeding 10%.

## 2. Dataset description and how the return flag was obtained
- Source, size, columns: see [`dataset_profile.md`](dataset_profile.md)
- The Kaggle file has one row per order with a genuine `returned` column (not synthesized) — explain
  that `return_request` Kafka events are *derived* from it with a simulated delay (see
  [`event_schema.md`](event_schema.md)), since the source has no separate returns log.

## 3. Architecture and technology choices
- Diagram: see the Mermaid flowchart in the main [README](../README.md)
- Why Kafka: decouples ingestion from storage, buffers bursts, makes the batch CSV look like a live
  order stream, and is replayable for reruns/demos.
- Why HDFS: durable, partitioned storage (`month=YYYY-MM/`) that MapReduce reads directly; partitioning
  limits job input to relevant months/weeks.
- Why MapReduce (Hadoop Streaming, Python) over Spark: scope fits a 3-day lab project and is the
  explicitly assigned tech stack; mention Spark Streaming as the natural real-time upgrade (section 6).
- Why a combiner: the return-rate aggregation (orders, returns counts) is associative and commutative,
  so partial sums can be combined before the shuffle — cuts network/shuffle volume. Good viva answer.

## 4. Implementation
- **Ingestion**: `ingestion/prep_data.py` (cleaning, column auto-detection), `producer.py` (replay as
  Kafka events), `consumer_to_hdfs.py` (micro-batched writer, idempotent via dedup)
- **Storage layout**: `/data/ecom/raw/month=YYYY-MM/batch-<epoch_ms>-<n>.jsonl`
- **MapReduce logic** (pseudocode):
  ```
  mapper:   for each event -> emit (week|category, "1,0") for orders, ("0,1") for return_requests
  combiner: sum orders/returns per key locally before shuffle
  reducer:  sum orders/returns per key -> rate = returns/orders*100 -> flag HIGH if rate>10% and orders>=10
  ```
- **Jobs**: Job 1 `week_category` (the headline weekly trend), Job 2 `category_region` (hotspot drill-down)

## 5. Results
- Paste the final `output/week_category.tsv` / `category_region.tsv` summary stats here
- Screenshots: HDFS UI (`:9870`), YARN job page (`:8088`), Kafka topic describe, dashboard
- Key insight(s): which category/region combination drives the most return cost, which weeks spike

## 6. Challenges, limitations, and future work
- Batch/micro-batch analytics, not true real-time (ingestion is streaming, analytics is not)
- Single-node pseudo-distributed cluster — not a real scaling test
- Return events are derived with a simulated delay, not observed directly
- Future: Spark Streaming for true real-time rate computation; Hive for ad-hoc SQL over the HDFS data;
  alerting (e.g. webhook/email) when a category-week crosses the threshold

## Appendix: viva Q&A prep
See "Likely viva questions" from the original project plan (Kafka vs. direct CSV read, combiner
correctness, why partition by week/month, real-time vs. batch, scaling path).
