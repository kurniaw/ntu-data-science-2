{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('raw_olist', 'products') }}
),
renamed AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.product_id') AS product_id,
        JSON_EXTRACT_SCALAR(data, '$.product_category_name') AS product_category_name,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.product_name_lenght'), '') AS INT64) AS product_name_length,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.product_description_lenght'), '') AS INT64) AS product_description_length,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.product_photos_qty'), '') AS INT64) AS product_photos_qty,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.product_weight_g'), '') AS INT64) AS product_weight_g,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.product_length_cm'), '') AS INT64) AS product_length_cm,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.product_height_cm'), '') AS INT64) AS product_height_cm,
        SAFE_CAST(NULLIF(JSON_EXTRACT_SCALAR(data, '$.product_width_cm'), '') AS INT64) AS product_width_cm
    FROM source
)
SELECT * FROM renamed
