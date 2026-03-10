{{ config(materialized='table') }}

WITH stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
stg_customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)
SELECT
    o.order_id,
    c.customer_unique_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date
FROM stg_orders o
LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
