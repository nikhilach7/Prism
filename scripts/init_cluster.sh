#!/usr/bin/env bash
# Run once after `docker compose up -d`: waits for HDFS, creates HDFS folders and the Kafka topic.
set -euo pipefail

echo "[1/3] waiting for HDFS to leave safe mode..."
for i in $(seq 1 30); do
  if docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; then break; fi
  sleep 4
done
docker exec namenode hdfs dfsadmin -safemode get

echo "[2/3] creating HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /data/ecom/raw /data/ecom/output
docker exec namenode hdfs dfs -ls /data/ecom

echo "[3/3] creating Kafka topic ecom.transactions (3 partitions)..."
for i in $(seq 1 15); do
  if docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
       --create --if-not-exists --topic ecom.transactions --partitions 3 --replication-factor 1; then break; fi
  sleep 3
done
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe --topic ecom.transactions

echo
echo "Cluster ready. Web UIs: HDFS http://localhost:9870   YARN http://localhost:8088"
echo "Sanity check for Streaming: docker exec nodemanager python3 --version"
