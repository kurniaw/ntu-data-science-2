{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_olist', 'customers') }}
),
renamed AS (
    SELECT
        customer_id,
        customer_unique_id,
        SAFE_CAST(customer_zip_code_prefix AS INT64) AS customer_zip_code,
        customer_city,
        customer_state
    FROM source
)
SELECT * FROM renamed
