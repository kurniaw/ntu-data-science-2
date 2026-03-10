{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_olist', 'orders') }}
),
renamed AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.order_id') AS order_id,
        JSON_EXTRACT_SCALAR(data, '$.customer_id') AS customer_id,
        JSON_EXTRACT_SCALAR(data, '$.order_status') AS order_status,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.order_purchase_timestamp'), '') AS TIMESTAMP) AS order_purchase_timestamp,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.order_approved_at'), '') AS TIMESTAMP) AS order_approved_at,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.order_delivered_carrier_date'), '') AS TIMESTAMP) AS order_delivered_carrier_date,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.order_delivered_customer_date'), '') AS TIMESTAMP) AS order_delivered_customer_date,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.order_estimated_delivery_date'), '') AS TIMESTAMP) AS order_estimated_delivery_date
    FROM source
)
SELECT * FROM renamed
