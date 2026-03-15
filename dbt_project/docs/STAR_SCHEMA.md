# Star Schema Design — Olist E-Commerce Data Warehouse

## 1. Executive Summary

This document defines the star schema for the Olist e-commerce analytics data warehouse hosted in BigQuery. The schema enables analysis of sales performance, customer behaviour, product trends, seller performance, and review sentiment.

**Source:** Olist Brazilian E-Commerce dataset (Kaggle)
**Warehouse:** Google BigQuery
**Transformation layer:** dbt
**Schema type:** Star schema with conformed dimensions

---

## 2. ER Diagram (Textual)

```
                          ┌─────────────────┐
                          │   dim_date      │
                          │─────────────────│
                          │ date_key (PK)   │
                          │ calendar_date   │
                          │ year/quarter/   │
                          │ month/week ...  │
                          └────────┬────────┘
                                   │ purchase_date_key
                                   │ approval_date_key
                                   │ delivery_date_key
           ┌───────────────────────┼───────────────────────┐
           │                       │                       │
┌──────────▼──────────┐  ┌─────────▼──────────┐  ┌────────▼────────────┐
│   fct_orders        │  │  fct_order_items   │  │ fct_product_reviews │
│─────────────────────│  │────────────────────│  │─────────────────────│
│ order_key (PK)      │  │ order_item_key (PK)│  │ review_key (PK)     │
│ order_id            │  │ order_id           │  │ review_id           │
│ customer_key (FK)───┼──┼────────────────────┼──┼─► dim_customer      │
│ purchase_date_key   │  │ product_key (FK)───┼──┼─► dim_product       │
│ customer_geo_key    │  │ seller_key (FK)    │  │ review_date_key     │
│ payment_type_key    │  │ seller_geo_key     │  │ review_score        │
│ total_revenue       │  │ price              │  │ review_sentiment    │
│ total_freight       │  │ freight_value      │  │ has_comment         │
│ delivery_days       │  │ total_item_value   │  └─────────────────────┘
│ is_on_time          │  └────────────────────┘
└─────────────────────┘

Shared dimension lookups:
  customer_key  ──► dim_customer   ──► dim_geography (via geography_key)
  product_key   ──► dim_product    ──► dim_product_category (via category_key)
  seller_key    ──► dim_seller     ──► dim_geography (via geography_key)
  *_geo_key     ──► dim_geography  (conformed)
  date_key      ──► dim_date       (conformed)
  payment_type_key ► dim_payment_type
```

---

## 3. Fact Tables

### 3.1 fct_orders

**Grain:** One row per order
**Business question answered:** What was the total revenue, order value, and delivery performance per order?

| Column | Type | Kind | Description |
|---|---|---|---|
| `order_key` | STRING | Surrogate PK | MD5 of `order_id` |
| `order_id` | STRING | Degenerate dim | Natural key from source |
| `customer_key` | INT64 | FK → dim_customer | Surrogate customer |
| `purchase_date_key` | INT64 | FK → dim_date | When order was placed (YYYYMMDD) |
| `approval_date_key` | INT64 | FK → dim_date | When order was approved |
| `carrier_date_key` | INT64 | FK → dim_date | When handed to carrier |
| `delivery_date_key` | INT64 | FK → dim_date | Actual delivery date |
| `estimated_delivery_date_key` | INT64 | FK → dim_date | Estimated delivery date |
| `customer_geography_key` | INT64 | FK → dim_geography | Customer zip location |
| `payment_type_key` | INT64 | FK → dim_payment_type | Primary payment method |
| `order_status` | STRING | Degenerate dim | delivered / shipped / canceled … |
| `order_item_count` | INT64 | Measure | Number of line items |
| `total_revenue` | FLOAT64 | Measure (additive) | SUM(price) across items |
| `total_freight` | FLOAT64 | Measure (additive) | SUM(freight_value) across items |
| `total_payment_value` | FLOAT64 | Measure (additive) | SUM(payment_value) from order_payments |
| `max_payment_installments` | INT64 | Measure | MAX(payment_installments) |
| `delivery_days` | INT64 | Derived measure | DATE_DIFF(delivered, purchased, DAY) |
| `approval_days` | INT64 | Derived measure | DATE_DIFF(approved, purchased, DAY) |
| `is_delivered` | BOOLEAN | Derived flag | order_status = 'delivered' |
| `is_on_time` | BOOLEAN | Derived flag | delivered_date <= estimated_date |

---

### 3.2 fct_order_items

**Grain:** One row per order line item
**Business question answered:** What products were sold, at what price, by which seller, on which date?

| Column | Type | Kind | Description |
|---|---|---|---|
| `order_item_key` | STRING | Surrogate PK | MD5 of order_id \|\| order_item_id |
| `order_id` | STRING | Degenerate dim | Links to fct_orders |
| `order_item_id` | INT64 | Degenerate dim | Position within order |
| `product_key` | INT64 | FK → dim_product | Surrogate product |
| `seller_key` | INT64 | FK → dim_seller | Surrogate seller |
| `seller_geography_key` | INT64 | FK → dim_geography | Seller zip location |
| `purchase_date_key` | INT64 | FK → dim_date | Order purchase date |
| `shipping_date_key` | INT64 | FK → dim_date | Shipping limit date |
| `price` | FLOAT64 | Measure (additive) | Item sale price |
| `freight_value` | FLOAT64 | Measure (additive) | Freight charged |
| `total_item_value` | FLOAT64 | Derived measure | price + freight_value |

---

### 3.3 fct_product_reviews

**Grain:** One row per review
**Business question answered:** How do customers rate products and what is the sentiment distribution?

| Column | Type | Kind | Description |
|---|---|---|---|
| `review_key` | STRING | Surrogate PK | MD5 of review_id |
| `review_id` | STRING | Degenerate dim | Natural key |
| `order_id` | STRING | Degenerate dim | Links to fct_orders |
| `customer_key` | INT64 | FK → dim_customer | Reviewer |
| `product_key` | INT64 | FK → dim_product | Reviewed product |
| `review_date_key` | INT64 | FK → dim_date | Review creation date |
| `review_score` | INT64 | Measure | 1–5 star rating |
| `review_sentiment` | STRING | Derived | positive (≥4) / neutral (3) / negative (≤2) |
| `has_comment` | BOOLEAN | Derived flag | review_comment_message IS NOT NULL |

---

## 4. Dimension Tables

### 4.1 dim_date — SCD Type 0 (Static)

**Conformed dimension** — shared across all fact tables.
**Surrogate key:** `date_key` (INT64, YYYYMMDD format)
**Natural key:** `calendar_date` (DATE)

| Column | Type | Description |
|---|---|---|
| `date_key` | INT64 | e.g. 20231201 |
| `calendar_date` | DATE | |
| `year` | INT64 | |
| `quarter` | INT64 | 1–4 |
| `month` | INT64 | 1–12 |
| `day_of_month` | INT64 | 1–31 |
| `day_of_week` | INT64 | 1=Monday, 7=Sunday |
| `week_of_year` | INT64 | ISO week |
| `day_name` | STRING | e.g. Monday |
| `month_name` | STRING | e.g. January |
| `is_weekend` | BOOLEAN | |
| `is_holiday` | BOOLEAN | Brazil public holidays (TODO) |
| `days_since_epoch` | INT64 | Days since 1970-01-01 |

---

### 4.2 dim_customer — SCD Type 2

**Tracks customer address changes over time.**
**Surrogate key:** `customer_key` (INT64, row_number)
**Natural keys:** `customer_unique_id` (business entity), `customer_id` (per-order transactional key)

| Column | Type | Description |
|---|---|---|
| `customer_key` | INT64 | Surrogate PK |
| `customer_unique_id` | STRING | Stable business key |
| `customer_id` | STRING | Per-order transactional key |
| `customer_zip_code` | INT64 | |
| `customer_city` | STRING | |
| `customer_state` | STRING | 2-letter BR state code |
| `geography_key` | INT64 | FK → dim_geography |
| `valid_from_date` | TIMESTAMP | SCD Type 2 start |
| `valid_to_date` | TIMESTAMP | SCD Type 2 end (NULL = current) |
| `is_current` | BOOLEAN | TRUE for active record |

> **Note:** In the Olist dataset, the same customer (`customer_unique_id`) may appear with different `customer_id` values across orders. SCD Type 2 captures any address changes between orders.

---

### 4.3 dim_product — SCD Type 1

**Products do not track historical changes** — overwrites on update.
**Surrogate key:** `product_key` (INT64, row_number)
**Natural key:** `product_id`

| Column | Type | Description |
|---|---|---|
| `product_key` | INT64 | Surrogate PK |
| `product_id` | STRING | Natural key |
| `product_category_name` | STRING | Portuguese category name |
| `category_key` | INT64 | FK → dim_product_category |
| `product_name_length` | INT64 | Character count |
| `product_description_length` | INT64 | Character count |
| `product_photos_qty` | INT64 | |
| `product_weight_g` | INT64 | |
| `product_length_cm` | INT64 | |
| `product_height_cm` | INT64 | |
| `product_width_cm` | INT64 | |
| `product_volume_cm3` | INT64 | Derived: length × height × width |

---

### 4.4 dim_seller — SCD Type 1

**Surrogate key:** `seller_key` (INT64, row_number)
**Natural key:** `seller_id`

| Column | Type | Description |
|---|---|---|
| `seller_key` | INT64 | Surrogate PK |
| `seller_id` | STRING | Natural key |
| `seller_zip_code` | INT64 | |
| `seller_city` | STRING | |
| `seller_state` | STRING | 2-letter BR state code |
| `geography_key` | INT64 | FK → dim_geography |

---

### 4.5 dim_geography — SCD Type 0 (Static, Conformed)

**Conformed dimension** — used by dim_customer, dim_seller, and directly by fct_orders / fct_order_items.
**Surrogate key:** `location_key` (INT64, row_number)
**Natural key:** `zip_code_prefix` + `state`

| Column | Type | Description |
|---|---|---|
| `location_key` | INT64 | Surrogate PK |
| `zip_code_prefix` | STRING | 5-digit Brazilian postal prefix |
| `city` | STRING | |
| `state` | STRING | 2-letter BR state code |
| `region` | STRING | North / Northeast / West / Southeast / South |
| `latitude` | FLOAT64 | |
| `longitude` | FLOAT64 | |
| `regional_sales_tier` | STRING | TODO: populate from aggregated sales |

---

### 4.6 dim_payment_type — SCD Type 0 (Static)

**Surrogate key:** `payment_type_key` (INT64, row_number)
**Natural key:** `payment_type_name`

| Column | Type | Description |
|---|---|---|
| `payment_type_key` | INT64 | Surrogate PK |
| `payment_type_name` | STRING | credit_card / debit_card / boleto / voucher |
| `payment_channel` | STRING | online / offline / other |
| `requires_installments` | BOOLEAN | TRUE for credit_card only |
| `payment_risk_level` | STRING | low / medium / high / unknown |

---

### 4.7 dim_product_category — SCD Type 2

**Tracks category reclassification over time.**
**Surrogate key:** `category_key` (INT64, row_number)
**Natural key:** `category_name` (Portuguese)

| Column | Type | Description |
|---|---|---|
| `category_key` | INT64 | Surrogate PK |
| `category_name` | STRING | Portuguese name (natural key) |
| `category_name_english` | STRING | English translation |
| `category_type` | STRING | Electronics / Fashion / Home / Health & Beauty / Other |
| `valid_from_date` | TIMESTAMP | SCD Type 2 start |
| `valid_to_date` | TIMESTAMP | SCD Type 2 end (NULL = current) |
| `is_current` | BOOLEAN | TRUE for active record |

---

## 5. Derived Metrics Catalogue

| Metric | Formula | Fact Table |
|---|---|---|
| **Revenue** | SUM(total_revenue) | fct_orders |
| **AOV** (Average Order Value) | SUM(total_revenue) / COUNT(DISTINCT order_key) | fct_orders |
| **Freight Rate** | SUM(total_freight) / SUM(total_revenue) | fct_orders |
| **On-Time Delivery Rate** | COUNTIF(is_on_time) / COUNT(*) | fct_orders |
| **Avg Delivery Days** | AVG(delivery_days) | fct_orders |
| **Revenue per Category** | SUM(price) GROUP BY category_name_english | fct_order_items + dim_product + dim_product_category |
| **Revenue per Seller** | SUM(price) GROUP BY seller_id | fct_order_items + dim_seller |
| **Avg Review Score** | AVG(review_score) | fct_product_reviews |
| **Positive Review Rate** | COUNTIF(review_sentiment='positive') / COUNT(*) | fct_product_reviews |
| **Items per Order** | AVG(order_item_count) | fct_orders |
| **Product Volume** | product_length_cm × product_height_cm × product_width_cm | dim_product |
| **Customer LTV** | SUM(total_revenue) per customer_unique_id | fct_orders + dim_customer |

---

## 6. Example Analytical Queries (BigQuery SQL)

### Q1 — Monthly Revenue and AOV

```sql
SELECT
  d.year,
  d.month,
  d.month_name,
  COUNT(DISTINCT o.order_key)          AS order_count,
  SUM(o.total_revenue)                 AS total_revenue,
  SUM(o.total_revenue)
    / COUNT(DISTINCT o.order_key)      AS avg_order_value,
  AVG(o.delivery_days)                 AS avg_delivery_days,
  COUNTIF(o.is_on_time)
    / COUNT(*)                         AS on_time_rate
FROM `stg_olist_analytics.fct_orders` o
JOIN `stg_olist_analytics.dim_date` d
  ON o.purchase_date_key = d.date_key
WHERE o.is_delivered = TRUE
GROUP BY 1, 2, 3
ORDER BY 1, 2;
```

---

### Q2 — Top 10 Product Categories by Revenue

```sql
SELECT
  pc.category_name_english,
  pc.category_type,
  COUNT(DISTINCT oi.order_id)          AS orders,
  SUM(oi.price)                        AS revenue,
  AVG(oi.price)                        AS avg_item_price,
  SUM(oi.freight_value)                AS total_freight
FROM `stg_olist_analytics.fct_order_items` oi
JOIN `stg_olist.dim_products` p
  ON oi.product_key = p.product_key
JOIN `stg_olist_analytics.dim_product_category` pc
  ON p.category_key = pc.category_key
WHERE pc.is_current = TRUE
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 10;
```

---

### Q3 — Seller Performance by State

```sql
SELECT
  g.state,
  g.region,
  COUNT(DISTINCT s.seller_key)         AS seller_count,
  COUNT(DISTINCT oi.order_id)          AS order_count,
  SUM(oi.price)                        AS revenue,
  AVG(oi.price)                        AS avg_price
FROM `stg_olist_analytics.fct_order_items` oi
JOIN `stg_olist.dim_sellers` s
  ON oi.seller_key = s.seller_key
JOIN `stg_olist_analytics.dim_geography` g
  ON s.geography_key = g.location_key
GROUP BY 1, 2
ORDER BY revenue DESC;
```

---

### Q4 — Review Sentiment by Category

```sql
SELECT
  pc.category_name_english,
  COUNTIF(r.review_sentiment = 'positive')  AS positive,
  COUNTIF(r.review_sentiment = 'neutral')   AS neutral,
  COUNTIF(r.review_sentiment = 'negative')  AS negative,
  COUNT(*)                                   AS total_reviews,
  ROUND(AVG(r.review_score), 2)             AS avg_score,
  ROUND(
    COUNTIF(r.review_sentiment = 'positive') / COUNT(*) * 100, 1
  )                                          AS positive_pct
FROM `stg_olist_analytics.fct_product_reviews` r
JOIN `stg_olist.dim_products` p
  ON r.product_key = p.product_key
JOIN `stg_olist_analytics.dim_product_category` pc
  ON p.category_key = pc.category_key
WHERE pc.is_current = TRUE
GROUP BY 1
HAVING total_reviews >= 50
ORDER BY avg_score DESC;
```

---

### Q5 — Customer RFM Segmentation (as of today)

```sql
WITH rfm AS (
  SELECT
    c.customer_unique_id,
    c.customer_state,
    MAX(d.calendar_date)                             AS last_order_date,
    DATE_DIFF(CURRENT_DATE, MAX(d.calendar_date), DAY) AS recency_days,
    COUNT(DISTINCT o.order_key)                       AS frequency,
    SUM(o.total_revenue)                              AS monetary
  FROM `stg_olist_analytics.fct_orders` o
  JOIN `stg_olist.dim_customers` c
    ON o.customer_key = c.customer_key
  JOIN `stg_olist_analytics.dim_date` d
    ON o.purchase_date_key = d.date_key
  WHERE c.is_current = TRUE
    AND o.is_delivered = TRUE
  GROUP BY 1, 2
)
SELECT
  *,
  CASE
    WHEN recency_days <= 90  AND frequency >= 3 AND monetary >= 500 THEN 'Champions'
    WHEN recency_days <= 180 AND frequency >= 2                      THEN 'Loyal'
    WHEN recency_days <= 90                                          THEN 'Recent'
    WHEN recency_days > 365                                          THEN 'Churned'
    ELSE 'At Risk'
  END AS rfm_segment
FROM rfm
ORDER BY monetary DESC;
```

---

## 7. Data Warehouse Best Practices Applied

| Practice | Application |
|---|---|
| **Surrogate keys** | All dimensions carry an INT64 or STRING surrogate key; natural keys are preserved as degenerate dimensions in facts |
| **Conformed dimensions** | `dim_date` and `dim_geography` are shared across all fact tables without modification |
| **Grain declaration** | Each fact table has an explicit, documented grain (order / line item / review) |
| **Additive measures** | Revenue, freight, and count measures are fully additive across all dimensions |
| **SCD Type 2** | `dim_customer` and `dim_product_category` track historical changes via `valid_from_date`, `valid_to_date`, `is_current` |
| **Degenerate dimensions** | `order_id`, `order_item_id`, `review_id` stored in fact tables to support drill-through without a dimension table |
| **Derived metrics in the layer** | Delivery days, is_on_time, total_item_value, review_sentiment computed at model build time — no runtime calculation needed for dashboards |
| **Null-safe casting** | All staging models use `SAFE_CAST` to prevent pipeline failures on malformed source data |
| **Surrogate key generation** | `dbt_utils.generate_surrogate_key()` used for fact surrogate keys; `row_number()` used for dimension surrogate keys |
| **BigQuery-native SQL** | `GENERATE_DATE_ARRAY`, `FORMAT_DATE`, `DATE_DIFF`, `EXTRACT(ISOWEEK)` used throughout instead of PostgreSQL-specific functions |

---

## 8. Implementation Gap Analysis

This section maps the designed schema to the current dbt implementation state.

### Fully Implemented ✅

| Object | dbt model |
|---|---|
| dim_date | `models/analytics/dims/dim_date.sql` |
| dim_geography | `models/analytics/dims/dim_geography.sql` |
| dim_payment_type | `models/analytics/dims/dim_payment_type.sql` |
| dim_product_category | `models/analytics/dims/dim_product_category.sql` |
| fct_product_reviews | `models/analytics/facts/fct_product_reviews.sql` |

### Partially Implemented ⚠️

| Object | Current state | Gap |
|---|---|---|
| dim_customer | `models/core/dim_customers.sql` — uses `customer_unique_id` as natural key | Missing: surrogate key (`customer_key`), `geography_key` FK, SCD Type 2 columns |
| dim_product | `models/core/dim_products.sql` — passes through staging | Missing: surrogate key (`product_key`), `category_key` FK to dim_product_category, `product_volume_cm3` |
| dim_seller | `models/core/dim_sellers.sql` — passes through staging | Missing: surrogate key (`seller_key`), `geography_key` FK |
| fct_orders | `models/core/fct_orders.sql` — basic order join | Missing: surrogate key (`order_key`), all derived measures, all FK references to analytics dims |
| fct_order_items | `models/core/fct_order_items.sql` — basic line item | Missing: surrogate key, `product_key`/`seller_key` surrogate FKs, derived `total_item_value` |

### Not Yet Implemented ❌

| Object | Notes |
|---|---|
| Brazil public holidays in dim_date | `is_holiday` column exists but is hardcoded `false` |
| `regional_sales_tier` in dim_geography | Placeholder `NULL` — requires aggregated sales data |
| Payment-level grain fact | No `fct_payments` table; payment metrics are rolled up to order level in `fct_orders` |
| dbt snapshot for SCD Type 2 | Snapshot strategy defined in `snapshots/` but not yet wired to dim_customer |

---

*Last updated: 2026-03-15*
