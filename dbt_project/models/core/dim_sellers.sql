{{ config(materialized='table') }}

WITH stg_sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
)
SELECT
    seller_id,
    seller_zip_code,
    seller_city,
    seller_state
FROM stg_sellers
