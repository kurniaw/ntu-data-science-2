{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_olist', 'order_items') }}
),
renamed AS (
    SELECT
        order_id,
        SAFE_CAST(order_item_id AS INT64) AS order_item_id,
        product_id,
        seller_id,
        SAFE_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
        SAFE_CAST(price AS FLOAT64) AS price,
        SAFE_CAST(freight_value AS FLOAT64) AS freight_value
    FROM source
)
SELECT * FROM renamed
