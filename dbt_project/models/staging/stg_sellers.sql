{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_olist', 'sellers') }}
),
renamed AS (
    SELECT
        seller_id,
        SAFE_CAST(seller_zip_code_prefix AS INT64) AS seller_zip_code,
        seller_city,
        seller_state
    FROM source
)
SELECT * FROM renamed
