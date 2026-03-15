# E-Commerce Analytics Data Warehouse - STAR SCHEMA DESIGN

## Executive Summary

This document defines the dimensional data warehouse design for analyzing e-commerce operations across customers, products, sellers, and order fulfillment. The schema uses a **star topology** with 3 fact tables and 8 dimension tables, optimized for analytical queries and reporting.

---

## 1. FACT TABLES

### **fct_orders** (One row per order)
Primary fact table capturing order-level metrics.

| Column | Type | Role | Notes |
|--------|------|------|-------|
| `order_key` | INT | PK, Surrogate | Auto-increment |
| `order_id` | VARCHAR | Degenerate Dim | Business key |
| `customer_key` | INT | FK | → dim_customers |
| `product_key` | INT | FK | → dim_products |
| `seller_key` | INT | FK | → dim_sellers |
| `order_date_key` | INT | FK | → dim_date (order date) |
| `delivery_date_key` | INT | FK | → dim_date (delivery date) |
| `order_amount` | DECIMAL(10,2) | Measure | Total order value |
| `total_freight` | DECIMAL(10,2) | Measure | Shipping cost |
| `payment_method` | VARCHAR | Degenerate Dim | credit_card, boleto, etc. |
| `order_status` | VARCHAR | Degenerate Dim | delivered, cancelled, etc. |
| `review_score` | INT | Measure | 1-5 star rating (if reviewed) |

---

### **fct_order_items** (One row per line item)
Atomic fact table for detailed order line analysis.

| Column | Type | Role | Notes |
|--------|------|------|-------|
| `order_item_key` | INT | PK, Surrogate | Auto-increment |
| `order_key` | INT | FK | → fct_orders |
| `order_id` | VARCHAR | Degenerate Dim | Keep for convenience |
| `order_item_id` | INT | Degenerate Dim | Line item number |
| `product_key` | INT | FK | → dim_products |
| `seller_key` | INT | FK | → dim_sellers |
| `order_date_key` | INT | FK | → dim_date |
| `item_price` | DECIMAL(10,2) | Measure | Unit price × quantity |
| `item_freight_value` | DECIMAL(10,2) | Measure | Shipping for this item |
| `item_total_value` | DECIMAL(10,2) | Measure | Price + freight |

---

### **fct_product_reviews** (One row per review)
Review metrics fact table.

| Column | Type | Role | Notes |
|--------|------|------|-------|
| `review_key` | INT | PK, Surrogate | Auto-increment |
| `review_id` | VARCHAR | Natural Key | Business key |
| `order_key` | INT | FK | → fct_orders |
| `product_key` | INT | FK | → dim_products |
| `customer_key` | INT | FK | → dim_customers |
| `review_date_key` | INT | FK | → dim_date |
| `review_score` | INT | Measure | 1-5 stars |
| `review_sentiment` | VARCHAR | Measure | positive/neutral/negative |

---

## 2. DIMENSION TABLES

### **dim_customers** (Slowly Changing - Type 2)
Customer master dimension with historical tracking.

| Column | Type | Role | Notes |
|--------|------|------|-------|
| `customer_key` | INT | PK, Surrogate | |
| `customer_id` | VARCHAR | Natural Key | Operational ID |
| `customer_unique_id` | VARCHAR | Natural Key | Unique identifier |
| `customer_city` | VARCHAR | Attribute | |
| `customer_state` | VARCHAR | Attribute | |
| `customer_zip_code_prefix` | VARCHAR | Attribute | |
| `customer_location_key` | INT | FK | → dim_geography |
| `customer_first_order_date` | DATE | Attribute | Onboarding date |
| `customer_lifetime_value` | DECIMAL(12,2) | Computed | Sum of all orders |
| `valid_from_date` | DATE | SCD Type 2 | Start date for this row |
| `valid_to_date` | DATE | SCD Type 2 | End date (NULL if current) |
| `is_current` | BOOLEAN | SCD Type 2 | TRUE if latest version |

**SCD Strategy:** Type 2 - Maintains full history of customer changes (location, status).

---

### **dim_products** (Slowly Changing - Type 2)
Product master dimension with physical characteristics.

| Column | Type | Role | Notes |
|--------|------|------|-------|
| `product_key` | INT | PK, Surrogate | |
| `product_id` | VARCHAR | Natural Key | |
| `product_name` | VARCHAR(255) | Attribute | |
| `product_category_key` | INT | FK | → dim_product_category |
| `product_category_name_english` | VARCHAR | Attribute | Denormalized for speed |
| `product_description_length` | INT | Attribute | Char count |
| `product_photos_quantity` | INT | Attribute | |
| `product_weight_g` | INT | Attribute | In grams |
| `product_height_cm` | DECIMAL(5,1) | Attribute | |
| `product_width_cm` | DECIMAL(5,1) | Attribute | |
| `product_length_cm` | DECIMAL(5,1) | Attribute | |
| `product_volume_cm3` | DECIMAL(10,2) | Computed | Height × Width × Length |
| `product_density` | DECIMAL(8,4) | Computed | Weight / Volume |
| `product_launch_date` | DATE | Attribute | When product was added |
| `valid_from_date` | DATE | SCD Type 2 | |
| `valid_to_date` | DATE | SCD Type 2 | |
| `is_current` | BOOLEAN | SCD Type 2 | |

**SCD Strategy:** Type 2 - Tracks category changes, description updates.

---

### **dim_product_category** (Slowly Changing - Type 2)
Product category dimension with translations.

| Column | Type | Role | Notes |
|--------|------|------|-------|
| `category_key` | INT | PK, Surrogate | |
| `category_name` | VARCHAR | Natural Key | Portuguese original |
| `category_name_english` | VARCHAR | Natural Key | English translation |
| `category_type` | VARCHAR | Attribute | Derived grouping |
| `valid_from_date` | DATE | SCD Type 2 | |
| `valid_to_date` | DATE | SCD Type 2 | |
| `is_current` | BOOLEAN | SCD Type 2 | |

---

### **dim_sellers** (Slowly Changing - Type 2)
Seller/merchant master dimension.

| Column | Type | Role | Notes |
|--------|------|------|-------|
| `seller_key` | INT | PK, Surrogate | |
| `seller_id` | VARCHAR | Natural Key | |
| `seller_city` | VARCHAR | Attribute | |
| `seller_state` | VARCHAR | Attribute | |
| `seller_zip_code_prefix` | VARCHAR | Attribute | |
| `seller_location_key` | INT | FK | → dim_geography |
| `seller_onboarded_date` | DATE | Attribute | |
| `seller_status` | VARCHAR | Attribute | active/inactive |
| `seller_rating` | DECIMAL(3,2) | Computed | Avg review score |
| `total_products_sold` | INT | Computed | Lifetime orders |
| `valid_from_date` | DATE | SCD Type 2 | |
| `valid_to_date` | DATE | SCD Type 2 | |
| `is_current` | BOOLEAN | SCD Type 2 | |

**SCD Strategy:** Type 2 - Tracks status changes, rating updates.

---

### **dim_geography** (No SCD - Type 1)
Geographic location dimension (cities/zip codes).

| Column | Type | Role | Notes |
|--------|------|------|-------|
| `location_key` | INT | PK, Surrogate | |
| `zip_code_prefix` | VARCHAR | Natural Key | |
| `city` | VARCHAR | Attribute | |
| `state` | VARCHAR | Attribute | |
| `latitude` | DECIMAL(9,6) | Attribute | |
| `longitude` | DECIMAL(9,6) | Attribute | |
| `region` | VARCHAR | Computed | North/Northeast/West/South/Southeast |
| `regional_sales_tier` | VARCHAR | Computed | Stratified by sales volume |

**SCD Strategy:** Type 1 - Geographic data is static.

---

### **dim_date** (No SCD - Type 1)
Standard calendar dimension for temporal analysis.

| Column | Type | Role | Notes |
|--------|------|------|-------|
| `date_key` | INT | PK, Surrogate | YYYYMMDD format |
| `calendar_date` | DATE | Natural Key | Actual date |
| `year` | INT | Attribute | |
| `quarter` | INT | Attribute | 1-4 |
| `month` | INT | Attribute | 1-12 |
| `day_of_month` | INT | Attribute | 1-31 |
| `day_of_week` | INT | Attribute | 1-7 (Mon-Sun) |
| `week_of_year` | INT | Attribute | ISO week number |
| `day_name` | VARCHAR | Attribute | Monday, Tuesday, etc. |
| `month_name` | VARCHAR | Attribute | January, February, etc. |
| `is_weekend` | BOOLEAN | Attribute | TRUE if Sat/Sun |
| `is_holiday` | BOOLEAN | Attribute | Brazil holidays |

**Coverage:** Pre-load 5 years back, 5 years forward.

---

### **dim_payment_type** (No SCD - Type 1)
Payment method dimension.

| Column | Type | Role | Notes |
|--------|------|------|-------|
| `payment_type_key` | INT | PK, Surrogate | |
| `payment_type_name` | VARCHAR | Natural Key | credit_card, debit_card, boleto |
| `payment_channel` | VARCHAR | Attribute | online/offline |
| `requires_installments` | BOOLEAN | Attribute | Can payments be split? |
| `payment_risk_level` | VARCHAR | Attribute | low/medium/high |

---

## 3. SCHEMA DIAGRAM

```
                        dim_date
                          |
         _________________|_________________
         |                 |                 |
    order_date_key   delivery_date_key  review_date_key
         |                 |                 |
    fct_orders  ←────────────────────  fct_product_reviews
         |                                   |
         |--→ customer_key ───→ dim_customers
         |                           |
         |                  customer_location_key
         |                           |
         |--→ product_key ───→ dim_products
         |                           |
         |             product_category_key
         |                           |
         |--→ seller_key ───→ dim_sellers ──→ dim_geography
         |
    fct_order_items
         |--→ order_key ───→ fct_orders
         |--→ product_key ───→ dim_products
         |--→ seller_key ───→ dim_sellers
         |--→ order_date_key ───→ dim_date
```

---

## 4. KEY METRICS & CALCULATIONS

### Revenue Metrics
- **Total Revenue** = SUM(item_price)
- **Revenue with Freight** = SUM(item_price + item_freight_value)
- **Average Order Value (AOV)** = Total Revenue / Order Count
- **Revenue per Seller** = Grouped by seller
- **Revenue per Category** = Grouped by product category

### Customer Metrics
- **Customer Lifetime Value (CLV)** = SUM of all orders by customer
- **Purchase Frequency** = COUNT(distinct orders) per customer
- **Repeat Purchase Rate** = Customers with 2+ orders / Total customers
- **Customer Cohort Retention** = Tracked by first_purchase cohort

### Product Metrics
- **Product Volume** = Height × Width × Length (cm³)
- **Product Density** = Weight / Volume (kg/m³)
- **Average Product Rating** = AVG(review_score)
- **Sales Velocity** = Order count / Days since launch
- **Stock Turnover** = Sales / Average inventory

### Seller Metrics
- **Fulfillment Time** = AVG(delivery_date - order_date)
- **Seller Rating** = AVG(review_score) for seller's orders
- **Gross Merchandise Value (GMV)** = SUM(order value)
- **Order Volume** = COUNT(orders)

### Operational Metrics
- **Order Approval Time** = order_approval_at - order_purchase_timestamp
- **Payment Authorization Rate** = Successful payments / Total payments
- **Installment Usage** = Orders with installments / Total orders

---

## 5. EXAMPLE QUERIES

### Monthly Revenue by Category
```sql
SELECT 
    EXTRACT(YEAR FROM dd.calendar_date) as year,
    EXTRACT(MONTH FROM dd.calendar_date) as month,
    dpc.category_name_english,
    COUNT(DISTINCT foi.order_key) as order_count,
    ROUND(SUM(foi.item_price), 2) as revenue,
    ROUND(AVG(foi.item_price), 2) as avg_price,
    ROUND(AVG(COALESCE(fpr.review_score, 0)), 2) as avg_rating
FROM fct_order_items foi
JOIN dim_products dp ON foi.product_key = dp.product_key
JOIN dim_product_category dpc ON dp.product_category_key = dpc.category_key
JOIN dim_date dd ON foi.order_date_key = dd.date_key
LEFT JOIN fct_product_reviews fpr ON foi.order_key = fpr.order_key
WHERE dp.is_current = TRUE AND dpc.is_current = TRUE
GROUP BY EXTRACT(YEAR FROM dd.calendar_date), 
         EXTRACT(MONTH FROM dd.calendar_date),
         dpc.category_name_english
ORDER BY year DESC, month DESC;
```

### Top 10 Sellers YTD
```sql
SELECT 
    TOP 10
    ds.seller_id,
    ds.seller_city,
    ds.seller_state,
    COUNT(DISTINCT foi.order_key) as order_count,
    ROUND(SUM(foi.item_price + foi.item_freight_value), 2) as gmv,
    ROUND(AVG(COALESCE(fpr.review_score, 0)), 2) as avg_rating,
    COUNT(DISTINCT CASE WHEN fpr.review_score >= 4 THEN fpr.review_key END) as positive_reviews
FROM dim_sellers ds
JOIN fct_order_items foi ON ds.seller_key = foi.seller_key
JOIN dim_date dd ON foi.order_date_key = dd.date_key
LEFT JOIN fct_product_reviews fpr ON foi.order_key = fpr.order_key
WHERE ds.is_current = TRUE AND EXTRACT(YEAR FROM dd.calendar_date) = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY ds.seller_id, ds.seller_city, ds.seller_state
ORDER BY gmv DESC;
```

### Customer Segmentation (RFM)
```sql
WITH customer_summary AS (
    SELECT 
        dc.customer_key,
        dc.customer_id,
        MAX(dd.calendar_date) as last_purchase,
        DATEDIFF(day, MAX(dd.calendar_date), CURRENT_DATE) as days_since_purchase,
        COUNT(DISTINCT foi.order_key) as purchase_count,
        ROUND(SUM(foi.item_price + foi.item_freight_value), 2) as lifetime_value
    FROM dim_customers dc
    LEFT JOIN fct_order_items foi ON dc.customer_key = foi.product_key
    LEFT JOIN dim_date dd ON foi.order_date_key = dd.date_key
    WHERE dc.is_current = TRUE
    GROUP BY dc.customer_key, dc.customer_id
)
SELECT 
    customer_key,
    customer_id,
    last_purchase,
    days_since_purchase,
    purchase_count,
    lifetime_value,
    CASE 
        WHEN days_since_purchase <= 30 AND purchase_count >= 3 THEN 'Champions'
        WHEN days_since_purchase <= 90 AND purchase_count >= 2 THEN 'Loyal Customers'
        WHEN days_since_purchase <= 180 THEN 'At Risk'
        ELSE 'Lost'
    END as customer_segment
FROM customer_summary
WHERE lifetime_value > 0
ORDER BY lifetime_value DESC;
```

---

## 6. IMPLEMENTATION BEST PRACTICES

### 1. **Conformed Dimensions**
- `dim_date` is shared across all fact tables
- `dim_geography` is shared between customers and sellers
- Ensures consistent metrics across the enterprise

### 2. **Slowly Changing Dimension Strategy**

**Type 1 (Overwrite):** `dim_date`, `dim_geography`, `dim_payment_type`
- Historical changes not tracked
- Latest values only

**Type 2 (Track History):** `dim_customers`, `dim_products`, `dim_sellers`, `dim_product_category`
- Full historical record maintained
- Use `valid_from_date`, `valid_to_date`, `is_current` flags
- Enables time-based analysis and what-if scenarios

### 3. **Indexing Strategy**
```sql
-- Primary Keys (Clustered)
CREATE CLUSTERED INDEX ix_fct_orders_pk ON fct_orders(order_key);
CREATE CLUSTERED INDEX ix_fct_order_items_pk ON fct_order_items(order_item_key);

-- Foreign Keys (Non-Clustered)
CREATE NONCLUSTERED INDEX ix_fct_orders_customer ON fct_orders(customer_key);
CREATE NONCLUSTERED INDEX ix_fct_orders_date ON fct_orders(order_date_key);

-- SCD Type 2 Lookups
CREATE NONCLUSTERED INDEX ix_dim_customers_current 
  ON dim_customers(customer_id, is_current);

-- Composite for common queries
CREATE NONCLUSTERED INDEX ix_fct_order_items_composite 
  ON fct_order_items(order_date_key, product_key, seller_key);
```

### 4. **Partitioning for Performance**
- Partition `fct_order_items` by `order_date_key` (monthly or quarterly)
- Partition `fct_product_reviews` by `review_date_key`
- Enables fast historical purges and query optimization

### 5. **Data Quality Rules**
- NULL allowed in `fct_product_reviews` for non-reviewed items
- NULL allowed in `review_score` for orders without reviews
- SCD Type 2: Ensure no overlapping date ranges for same natural key
- Validate that `valid_from_date` < `valid_to_date`

### 6. **Refresh Schedule**
- **Daily:** Fact tables (orders, items, reviews)
- **Weekly:** Dimension tables (customers, products, sellers)
- **Monthly:** SCD Type 2 changes (new versions vs. overwrites)

---

## 7. ROLE-BASED ACCESS (Optional)

```sql
-- Analytics Users: Read-only on facts and dimensions
GRANT SELECT ON fct_orders TO role_analyst;
GRANT SELECT ON fct_order_items TO role_analyst;

-- BI Dashboard Users: Access to conformed views
GRANT SELECT ON v_orders_analytics TO role_bi_dashboard;

-- Data Engineers: Full access for ETL/maintenance
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES TO role_data_engineer;
```

---

## 8. MIGRATION CHECKLIST

- [ ] Create `dim_date` and pre-load calendar (current year ±5 years)
- [ ] Create `dim_geography` from geolocation data
- [ ] Create `dim_customers` with SCD Type 2 logic
- [ ] Create `dim_products` and `dim_product_category`
- [ ] Create `dim_sellers` with SCD Type 2 logic
- [ ] Create `dim_payment_type` reference data
- [ ] Load `fct_orders` from source
- [ ] Load `fct_order_items` from source
- [ ] Load `fct_product_reviews` from source
- [ ] Create all indexes
- [ ] Set up partitioning
- [ ] Validate row counts and data integrity
- [ ] Create conformed views (v_orders_analytics)
- [ ] Grant permissions
- [ ] Set up automated refresh schedule (Meltano/dbt/SQL Agent)
- [ ] Document in data dictionary

---

## 9. FUTURE ENHANCEMENTS

- **Product Returns Fact Table:** Add `fct_returns` for return analysis
- **Marketing Campaigns Dimension:** Add campaign tracking if available
- **Inventory Dimension:** Track stock levels over time
- **Forecast Fact Table:** Store predictions for demand planning
- **Price History Dimension:** Track product price changes (SCD Type 2)
- **Promotional Calendar:** Holidays, sales events, discount periods
- **Customer Lifetime Value Metrics:** Pre-computed for BI performance

---

## Questions & Support

For questions on the schema design or implementation, refer to:
- Schema grain definitions → Section 1
- Dimensional attributes → Section 2
- Example queries → Section 5
- Best practices → Section 6

