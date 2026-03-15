# dbt Star Schema Analytics - Quick Start Guide

## Overview

This dbt project implements a **dimensional data warehouse (star schema)** for e-commerce analytics. It transforms raw OLIST data into analytics-ready fact and dimension tables.

## Project Structure

```
dbt_project/
├── models/
│   ├── staging/          # Raw data cleaning (existing)
│   ├── core/             # Business transformations (existing)
│   └── analytics/        # ⭐ NEW: Star schema dimensions & facts
│       ├── dims/         # 7 dimension tables
│       ├── facts/        # 3 fact tables
│       └── README.md     # Detailed schema documentation
├── dbt_project.yml       # dbt configuration
└── profiles.yml          # Database connection
```

## Quick Setup

### 1. Install dbt Dependencies
```bash
cd dbt_project
dbt deps
```

### 2. Seed Reference Data (Optional)
```bash
dbt seed
```

### 3. Run Models in Order
```bash
# First: Dimensions (they have no dependencies on facts)
dbt run --select analytics.dims.*

# Then: Facts (they depend on dimensions)
dbt run --select analytics.facts.*

# Or run all analytics models:
dbt run --select analytics.*
```

### 4. Test Data Quality
```bash
dbt test --select analytics.*
```

## Model Dependencies

```
sources (raw_olist)
    ↓
dim_date ────────────┐
dim_geography ───────┼──→ dim_customers ──┐
dim_payment_type ─┐  │                     │
                  │  ├──→ dim_products ───┼──→ dim_product_category
                  │  │                     │
                  └──┼──→ dim_sellers ─────┤
                     │                     ↓
                     ├──→ fct_orders ──────→ fct_product_reviews
                     └──→ fct_order_items ─┘
```

## Key Models

### Dimensions (Look-up Tables)
| Table | Grain | Rows | Purpose |
|-------|-------|------|---------|
| `dim_date` | 1 per day | ~3,650 | Calendar dimension |
| `dim_geography` | 1 per zip | ~1,345 | Location dimension |
| `dim_payment_type` | 1 per type | ~5 | Payment method lookup |
| `dim_product_category` | 1 per category | ~72 | Product categories |
| `dim_products` | 1 per product | ~32K | Product master (SCD2) |
| `dim_customers` | 1 per customer | ~100K | Customer master (SCD2) |
| `dim_sellers` | 1 per seller | ~3.6K | Seller master (SCD2) |

### Facts (Measurement Tables)
| Table | Grain | Rows | Purpose |
|-------|-------|------|---------|
| `fct_orders` | 1 per order | ~99K | Order-level metrics |
| `fct_order_items` | 1 per line item | ~112K | Item-level metrics ⭐ |
| `fct_product_reviews` | 1 per review | ~99K | Review metrics |

## SCD Type 2 Tracking

Dimensions that track history:
- `dim_customers` (location changes)
- `dim_products` (category/attribute changes)
- `dim_sellers` (status changes)
- `dim_product_category` (name changes)

For current data only, filter with:
```sql
WHERE is_current = true
```

For historical analysis, use date ranges:
```sql
WHERE valid_from_date <= '2025-01-15'
  AND (valid_to_date > '2025-01-15' OR valid_to_date IS NULL)
```

## Running dbt Commands

### Full Build
```bash
dbt run --select analytics.*
```

### Incremental Refresh (facts only)
```bash
dbt run --select analytics.facts.*
```

### Generate Documentation
```bash
dbt docs generate
dbt docs serve
```

### Test Everything
```bash
dbt test --select analytics.*
```

### Debug Issues
```bash
# Check if source data exists
dbt source freshness

# Compile SQL to see generated code
dbt compile --select dim_customers

# Run with debug output
dbt run --select fct_orders --debug
```

## Configuration (dbt_project.yml)

Add this to your `dbt_project.yml`:

```yaml
models:
  ntu_data_science:
    analytics:
      materialized: table
      schema: analytics
      
      dims:
        materialized: table
        tags: ['dimension']
        +pre-hook: "{{ log('Running dimension: ' ~ this.name, info=true) }}"
      
      facts:
        materialized: table
        tags: ['fact']
        +pre-hook: "{{ log('Running fact: ' ~ this.name, info=true) }}"
```

## Performance Tuning

### Add Indexes
```sql
-- After tables are created, add indexes
CREATE INDEX idx_fct_orders_customer ON analytics.fct_orders(customer_key);
CREATE INDEX idx_fct_order_items_product ON analytics.fct_order_items(product_key);
```

### Partition Large Tables
```sql
-- Partition fact tables by date
CREATE TABLE analytics.fct_order_items
PARTITION BY RANGE (YEAR(order_date_key), MONTH(order_date_key))
AS ...
```

### Materialized Views
```yaml
# Create a denormalized view for BI tools
- name: v_orders_analytics
  materialized: view
  sql:
    select 
      o.*,
      c.customer_city,
      p.category_name_english,
      d.month_name
    from fct_orders o
    join dim_customers c on o.customer_key = c.customer_key
    join dim_products p on o.product_key = p.product_key
    join dim_date d on o.order_date_key = d.date_key
    where c.is_current and p.is_current
```

## Data Refresh Schedule

| Model | Frequency | Time | Latency |
|-------|-----------|------|---------|
| `fct_orders` | Daily | 2 AM | 0 days |
| `fct_order_items` | Daily | 2 AM | 0 days |
| `fct_product_reviews` | Daily | 2 AM | 0 days |
| `dim_customers` | Weekly | Monday 3 AM | 0-6 days |
| `dim_products` | Weekly | Monday 3 AM | 0-6 days |
| `dim_sellers` | Weekly | Monday 3 AM | 0-6 days |

## Common Queries

### Revenue by Category
```sql
SELECT 
  dpc.category_name_english,
  SUM(foi.item_price) as revenue,
  COUNT(DISTINCT foi.order_key) as orders
FROM fct_order_items foi
JOIN dim_products dp ON foi.product_key = dp.product_key
JOIN dim_product_category dpc ON dp.product_category_key = dpc.category_key
WHERE dp.is_current AND dpc.is_current
GROUP BY dpc.category_name_english
ORDER BY revenue DESC;
```

### Top 10 Sellers
```sql
SELECT 
  ds.seller_id,
  SUM(foi.item_total_value) as gmv,
  COUNT(DISTINCT foi.order_key) as orders,
  ROUND(AVG(fpr.review_score), 2) as rating
FROM dim_sellers ds
JOIN fct_order_items foi ON ds.seller_key = foi.seller_key
LEFT JOIN fct_product_reviews fpr ON foi.order_key = fpr.order_key
WHERE ds.is_current
GROUP BY ds.seller_id
ORDER BY gmv DESC
LIMIT 10;
```

## Troubleshooting

### Models Not Building
1. Check that raw sources exist in BigQuery
2. Verify database and schema in `profiles.yml`
3. Run `dbt source freshness` to check connectivity

### Foreign Keys Not Found
- Dimensions must be built before facts
- Run: `dbt run --select analytics.dims.* --then --select analytics.facts.*`

### SCD Type 2 Issues
- Always filter with `WHERE is_current = TRUE` for current data
- Use `valid_from_date` and `valid_to_date` for historical queries

## Next Steps

1. **Build the models**: `dbt run --select analytics.*`
2. **Test data quality**: `dbt test --select analytics.*`
3. **Review documentation**: `dbt docs generate && dbt docs serve`
4. **Create BI dashboards**: Connect your tool to `analytics.*` tables
5. **Monitor freshness**: Set up data freshness checks

## Resources

- 📖 [Complete Star Schema Design](../../../STAR_SCHEMA_DESIGN.md)
- 🔍 [Analytics Layer Details](README.md)
- 📚 [dbt Documentation](https://docs.getdbt.com/)
- 💡 [Star Schema Best Practices](https://en.wikipedia.org/wiki/Star_schema)

---

**Status:** ✅ Ready for development  
**Last Updated:** March 15, 2026  
**Version:** 1.0
