{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_olist', 'customers') }}
),
renamed AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.customer_id') AS customer_id,
        JSON_EXTRACT_SCALAR(data, '$.customer_unique_id') AS customer_unique_id,
        CAST(JSON_EXTRACT_SCALAR(data, '$.customer_zip_code_prefix') AS INT64) AS customer_zip_code,
        JSON_EXTRACT_SCALAR(data, '$.customer_city') AS customer_city,
        JSON_EXTRACT_SCALAR(data, '$.customer_state') AS customer_state
    FROM source
)
SELECT * FROM renamed
