{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_olist', 'sellers') }}
),
renamed AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.seller_id') AS seller_id,
        CAST(JSON_EXTRACT_SCALAR(data, '$.seller_zip_code_prefix') AS INT64) AS seller_zip_code,
        JSON_EXTRACT_SCALAR(data, '$.seller_city') AS seller_city,
        JSON_EXTRACT_SCALAR(data, '$.seller_state') AS seller_state
    FROM source
)
SELECT * FROM renamed
