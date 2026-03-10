{{ config(materialized='table') }}

WITH stg_customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)
SELECT
    customer_unique_id,
    ANY_VALUE(customer_zip_code) AS customer_zip_code,
    ANY_VALUE(customer_city) AS customer_city,
    ANY_VALUE(customer_state) AS customer_state
FROM stg_customers
GROUP BY customer_unique_id
