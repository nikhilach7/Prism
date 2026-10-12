#!/usr/bin/env bash
# End-to-end run (Day 2/3): stack up -> init -> stream into Kafka -> Kafka -> HDFS -> MapReduce -> export -> verify
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d
bash scripts/init_cluster.sh

echo "== start consumer (background) =="
python3 ingestion/consumer_to_hdfs.py --sink hdfs --idle-exit 25 > output/consumer.log 2>&1 &
CONSUMER_PID=$!

echo "== produce events =="
python3 ingestion/producer.py --rate "${RATE:-1000}"

wait $CONSUMER_PID
tail -n 3 output/consumer.log
docker exec namenode hdfs dfs -du -h /data/ecom/raw | head -n 30

echo "== MapReduce =="
bash scripts/run_jobs.sh

echo "== verify against pandas =="
python3 scripts/verify_with_pandas.py

echo "== dashboard: streamlit run dashboard/app.py =="
