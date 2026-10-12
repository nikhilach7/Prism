"""Shared settings for producer / consumer. Override with environment variables if needed."""
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ecom.transactions")
CONSUMER_GROUP = os.getenv("KAFKA_GROUP", "hdfs-writer")

HDFS_RAW_DIR = os.getenv("HDFS_RAW_DIR", "/data/ecom/raw")
NAMENODE_CONTAINER = os.getenv("NAMENODE_CONTAINER", "namenode")

# consumer micro-batching: flush when either limit is hit
BATCH_MAX_EVENTS = int(os.getenv("BATCH_MAX_EVENTS", "5000"))
BATCH_MAX_SECONDS = int(os.getenv("BATCH_MAX_SECONDS", "30"))

CLEAN_CSV = os.getenv("CLEAN_CSV", "data/clean/transactions_clean.csv")
