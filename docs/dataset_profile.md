# Dataset Profile

**Source:** Kaggle — [E-commerce Sales Transactions Dataset](https://www.kaggle.com/datasets/miadul/e-commerce-sales-transactions-dataset) by Arif Miah
(file: `ecommerce_sales_34500.csv`, 34,500 rows, 17 columns). The author labels it a synthetic dataset
designed to simulate realistic online shopping data — note this in the report; it is still the real,
unmodified Kaggle download (not a project-local fake), and `returned` is a genuine column, not derived.

Loaded into `data/raw/ecommerce_sales_34500.csv` and cleaned with `ingestion/prep_data.py`
(no `ALIASES` changes needed — column names matched automatically).

## `prep_data.py` profile output

```
loaded 34,500 rows, columns: ['order_id', 'customer_id', 'product_id', 'category', 'price', 'discount',
'quantity', 'payment_method', 'order_date', 'delivery_time_days', 'region', 'returned', 'total_amount',
'shipping_cost', 'profit_margin', 'customer_age', 'customer_gender']

kept 34,500 of 34,500 rows -> data/clean/transactions_clean.csv
date range : 2023-09-12 -> 2025-09-11
categories : 7  | overall return rate: 5.52%
             orders  returns  rate
category
Fashion        6254      518  8.28
Electronics    6180      451  7.30
Home           5487      310  5.65
Toys           4247      210  4.94
Sports         4171      206  4.94
Beauty         4103      155  3.78
Grocery        4058       53  1.31

categories above 10% overall: 0
NOTE: no category exceeds 10% overall - weekly cells may still cross it; check the weekly view.
```

## Weekly MapReduce output (`scripts/local_test.sh data/raw/ecommerce_sales_34500.csv`)

- 36,403 events (34,500 orders + 1,903 return requests)
- `week_category` groups: **735**
- Groups flagged `HIGH` (rate > 10%, >= 10 orders): **72**
- Cross-checked against pandas: **PASS**, all 735 groups match

No category is above 10% in the overall aggregate, but 72 of 735 weekly cells are — this is the
expected shape (per README "Known risks"): use the **weekly** view for the flagging demo and
dashboard headline, not the overall category table.

## Column mapping used (via `prep_data.py` ALIASES, auto-detected)

| Pipeline field | Source column |
|---|---|
| order_id | order_id |
| product_id | product_id |
| category | category |
| region | region |
| payment_method | payment_method |
| quantity | quantity |
| price | price |
| order_date | order_date |
| returned | returned (Yes/No) |

Unused source columns (available for extra analysis if time allows): `customer_id`, `discount`,
`delivery_time_days`, `total_amount`, `shipping_cost`, `profit_margin`, `customer_age`, `customer_gender`.
