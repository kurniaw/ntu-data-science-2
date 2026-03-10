{{ config(materialized='table') }}

WITH stg_order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
stg_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.seller_id,
    oi.shipping_limit_date,
    oi.price,
    oi.freight_value,
    o.order_purchase_timestamp
FROM stg_order_items oi
LEFT JOIN stg_orders o ON oi.order_id = o.order_id
