# Event schema (topic `ecom.transactions`, JSON, key = order_id)

| field | type | notes |
|---|---|---|
| event_id | string (uuid5) | deterministic from `order_id:event_type` -> duplicates are detectable |
| event_type | `order` \| `return_request` | one `order` per row; one extra `return_request` if the order was returned |
| order_id | string | |
| product_id | string | |
| category | string | Title-cased, trimmed |
| region | string | `Unknown` if the dataset has none |
| payment_method | string | `Unknown` if the dataset has none |
| quantity | int | > 0 |
| price | float | unit price, > 0 |
| order_date | `YYYY-MM-DD` | **grouping date** - returns are counted against the order's week/month |
| event_date | `YYYY-MM-DD` | when the event happened (= order_date for orders, +2..21 days for returns); used to order the replay |

HDFS layout: `/data/ecom/raw/month=YYYY-MM/batch-<epoch_ms>-<n>.jsonl`
Outputs: `/data/ecom/output/week_category/part-*`, `/data/ecom/output/category_region/part-*`

Note: the Kaggle file has one row per order with a return status, not a separate return-event
log, so `return_request` events are *derived* from it (the delay between order and return is simulated).
