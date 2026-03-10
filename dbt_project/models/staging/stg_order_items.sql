{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_olist', 'order_items') }}
),
renamed AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.order_id') AS order_id,
        CAST(JSON_EXTRACT_SCALAR(data, '$.order_item_id') AS INT64) AS order_item_id,
        JSON_EXTRACT_SCALAR(data, '$.product_id') AS product_id,
        JSON_EXTRACT_SCALAR(data, '$.seller_id') AS seller_id,
        CAST(JSON_EXTRACT_SCALAR(data, '$.shipping_limit_date') AS TIMESTAMP) AS shipping_limit_date,
        CAST(JSON_EXTRACT_SCALAR(data, '$.price') AS FLOAT64) AS price,
        CAST(JSON_EXTRACT_SCALAR(data, '$.freight_value') AS FLOAT64) AS freight_value
    FROM source
)
SELECT * FROM renamed
