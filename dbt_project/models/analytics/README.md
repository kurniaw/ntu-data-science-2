# Analytics Layer - Star Schema Data Warehouse

This folder contains the dimensional data warehouse models built with dbt. It implements a **STAR SCHEMA** optimized for e-commerce analytics.

## Folder Structure

```
analytics/
├── dims/                      # Dimension tables (look-up tables)
│   ├── dim_date.sql          # Calendar dimension (Type 1)
│   ├── dim_geography.sql      # Geographic dimension (Type 1)
│   ├── dim_payment_type.sql   # Payment method lookup (Type 1)
│   ├── dim_product_category.sql # Product categories (Type 2 SCD)
│   ├── dim_products.sql       # Products with specs (Type 2 SCD)
│   ├── dim_customers.sql      # Customers with metrics (Type 2 SCD)
│   └── dim_sellers.sql        # Sellers/merchants (Type 2 SCD)
├── facts/                     # Fact tables (measurement tables)
│   ├── fct_orders.sql         # Orders (grain: 1 row per order)
│   ├── fct_order_items.sql    # Line items (grain: 1 row per item)
│   └── fct_product_reviews.sql # Reviews (grain: 1 row per review)
├── analytics.yml              # Schema documentation and tests
└── README.md                  # This file
```

## Schema Overview

### Dimension Tables (Type 1 - Static)
These tables contain attributes that don't change:

- **dim_date**: Calendar dimension with years, quarters, months, weeks, holidays
- **dim_geography**: Cities and zip codes with latitude/longitude
- **dim_payment_type**: Payment method reference data

### Dimension Tables (Type 2 - SCD)
These tables track historical changes:

- **dim_product_category**: Product categories (tracks name changes)
- **dim_products**: Products with physical specifications and category (tracks attribute changes)
- **dim_customers**: Customers with location and purchase metrics (tracks location changes)
- **dim_sellers**: Sellers with performance and status (tracks status changes)

**SCD Type 2 Implementation:**
- `valid_from_date`: When this version became active
- `valid_to_date`: When this version became inactive (NULL = current)
- `is_current`: Boolean flag (TRUE = latest version)

### Fact Tables

1. **fct_orders** (Grain: 1 row per order)
   - Order-level metrics
   - Payment method and status
   - Links to customer, date dimensions
   - Useful for: order volume, by-order analysis

2. **fct_order_items** (Grain: 1 row per line item) ⭐ **Most Atomic**
   - Line item prices and freight
   - Product and seller metrics
   - Enables product and seller analysis
   - Useful for: revenue by product/category/seller, detailed analytics

3. **fct_product_reviews** (Grain: 1 row per review)
   - Review scores and sentiment
   - Links to product, customer, and order
   - Useful for: product ratings, customer satisfaction, sentiment analysis

## Key Design Decisions

### Surrogate Keys
All dimensions have artificial integer surrogate keys:
- Enables efficient joins and relationships
- Supports SCD Type 2 tracking
- Insulates from natural key changes

### Conformed Dimensions
**Shared dimensions** used across fact tables:
- `dim_date`: Used by all fact tables for consistent time analysis
- `dim_geography`: Used by both customers and sellers for location analysis

### Degenerate Dimensions
Business keys kept directly in fact tables:
- `order_id`, `order_item_id`, `review_id` in fact tables
- Avoids creating tiny dimension tables
- Enables efficient grouping and filtering

### Grain Definition
Each fact table has a clear grain (level of detail):
- **fct_orders**: One row = one order (if order had 3 items, still 1 row)
- **fct_order_items**: One row = one line item (if order had 3 items, 3 rows)
- **fct_product_reviews**: One row = one customer review

This prevents double-counting and supports different analytical questions.

### Additive Measures
All measures in facts are **additive** across any dimension:
- You can safely SUM revenue by customer, product, seller, or date
- Supports flexible aggregation in any BI tool

## Example Queries

### Revenue by Product Category (Monthly)
```sql
select 
  dd.year,
  dd.month_name,
  dpc.category_name_english,
  count(distinct foi.order_key) as order_count,
  sum(foi.item_price) as revenue,
  round(avg(foi.item_price), 2) as avg_price
from fct_order_items foi
join dim_products dp on foi.product_key = dp.product_key
join dim_product_category dpc on dp.product_category_key = dpc.category_key
join dim_date dd on foi.order_date_key = dd.date_key
where dp.is_current = true
  and dpc.is_current = true
group by dd.year, dd.month_name, dpc.category_name_english
order by dd.year, dd.month;
```

### Customer Segmentation (RFM)
```sql
with customer_metrics as (
  select 
    dc.customer_key,
    dc.customer_id,
    max(dd.calendar_date) as last_purchase,
    datediff(day, max(dd.calendar_date), current_date) as days_since,
    count(distinct foi.order_key) as purchase_count,
    sum(foi.item_total_value) as lifetime_value
  from dim_customers dc
  join fct_order_items foi on dc.customer_key = foi.product_key
  join dim_date dd on foi.order_date_key = dd.date_key
  where dc.is_current = true
  group by dc.customer_key, dc.customer_id
)
select 
  customer_id,
  last_purchase,
  days_since,
  purchase_count,
  lifetime_value,
  case 
    when days_since <= 30 and purchase_count >= 3 then 'Champions'
    when days_since <= 90 and purchase_count >= 2 then 'Loyal'
    when days_since <= 180 then 'At Risk'
    else 'Lost'
  end as segment
from customer_metrics
order by lifetime_value desc;
```

### Top Sellers by Revenue
```sql
select 
  ds.seller_id,
  ds.seller_city,
  ds.seller_state,
  count(distinct foi.order_key) as orders,
  sum(foi.item_total_value) as gmv,
  round(avg(fpr.review_score), 2) as avg_rating
from dim_sellers ds
join fct_order_items foi on ds.seller_key = foi.seller_key
left join fct_product_reviews fpr on foi.order_key = fpr.order_key
where ds.is_current = true
group by ds.seller_id, ds.seller_city, ds.seller_state
order by gmv desc
limit 10;
```

## Data Refresh Strategy

### Daily
- `fct_orders` - New orders processed daily
- `fct_order_items` - New line items
- `fct_product_reviews` - New reviews

### Weekly
- `dim_customers` - Customer demographics and metrics
- `dim_products` - Product catalog changes
- `dim_sellers` - Seller status and ratings
- `dim_product_category` - Category updates

### One-Time
- `dim_date` - Pre-loaded calendar (5 years back/forward)
- `dim_geography` - Zip codes (updated as needed)
- `dim_payment_type` - Payment methods (updated when added)

## Testing & Data Quality

All models include dbt tests for:
- **Uniqueness**: Surrogate keys and natural keys
- **Not-Null**: Critical columns (customer_key, order_id, etc.)
- **Referential Integrity**: Foreign keys (defined in analytics.yml)

Run tests with:
```bash
dbt test --select analytics.*
```

## SCD Type 2 Implementation Details

When a customer's location changes:
1. Previous row: `valid_to_date = change_date`, `is_current = false`
2. New row: `valid_from_date = change_date`, `valid_to_date = NULL`, `is_current = true`

This preserves history for questions like:
- "How did revenue change when customers in Region X increased?"
- "What was the product catalog on a specific historical date?"

## Conformed Dimensions Benefits

Using **dim_date** across all facts ensures:
✅ Consistent time-based comparisons
✅ Single source of truth for calendars
✅ Easier aggregation across different metrics
✅ Reduced storage (one calendar vs. replicated)

Using **dim_geography** for both customers and sellers enables:
✅ Geographic cross-sell analysis
✅ Regional performance metrics
✅ Proximity-based insights

## Next Steps

1. **Materialized Views**: Create views for common query patterns
   ```sql
   -- v_orders_analytics: All dimensions joined for BI tools
   ```

2. **Aggregation Tables**: Pre-aggregate for dashboard performance
   ```sql
   -- agg_revenue_by_category_day
   -- agg_seller_metrics_day
   ```

3. **Indexes**: Add partition and index recommendations

4. **Documentation**: Link to business glossary

5. **Monitoring**: Track table freshness and row counts

## Related Documentation

- [Star Schema Design Document](../../../STAR_SCHEMA_DESIGN.md) - Complete design specifications
- [dbt Documentation](https://docs.getdbt.com/) - dbt framework docs
- [Star Schema Best Practices](https://en.wikipedia.org/wiki/Star_schema) - Wikipedia reference

---

**Last Updated:** March 2026  
**Schema Version:** 1.0  
**Owner:** Data Warehouse Team
