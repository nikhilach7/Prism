#!/usr/bin/env bash
# Whole pipeline LOGIC without Docker (no Kafka, no Hadoop). Everyone should be able to run this on day 1.
#   prep -> events file -> local "HDFS" layout -> mapper | sort | reducer -> compare with pandas
# `sort` between mapper and reducer imitates Hadoop's shuffle-and-sort phase.
set -euo pipefail
cd "$(dirname "$0")/.."

RAW="${1:-data/raw/synthetic_ecommerce.csv}"
[[ -f "$RAW" ]] || python3 scripts/make_synthetic.py --out "$RAW"

python3 ingestion/prep_data.py --input "$RAW"
python3 ingestion/producer.py --dry-run data/events.jsonl
rm -rf data/hdfs_local
python3 ingestion/consumer_to_hdfs.py --source file:data/events.jsonl --sink local

mkdir -p output
cat data/hdfs_local/raw/month=*/*.jsonl > data/all_events.jsonl

{ printf "week\tcategory\torders\treturns\treturn_rate_pct\torder_value\treturned_value\tflag\n"
  python3 mapreduce/rr_mapper.py --group week_category < data/all_events.jsonl | sort \
    | python3 mapreduce/rr_reducer.py --threshold 10; } > output/week_category.tsv

{ printf "category\tregion\torders\treturns\treturn_rate_pct\torder_value\treturned_value\tflag\n"
  python3 mapreduce/rr_mapper.py --group category_region < data/all_events.jsonl | sort \
    | python3 mapreduce/rr_reducer.py --threshold 10; } > output/category_region.tsv

echo "week_category rows: $(($(wc -l < output/week_category.tsv) - 1)), HIGH-flagged: $(grep -c 'HIGH$' output/week_category.tsv || true)"
python3 scripts/verify_with_pandas.py
