{{
  config(
    materialized='table',
    schema='analytics',
    tags=['fact', 'daily']
  )
}}

-- Product reviews fact table: One row per review
-- Grain: One row per review
-- Contains review metrics for sentiment and satisfaction analysis

with reviews as (
  select
    r.review_id,
    r.order_id,
    SAFE_CAST(r.review_score AS INT64) as review_score,
    r.review_comment_message,
    r.review_creation_date,
    oi.product_id,
    o.customer_id
  from {{ source('raw_olist', 'order_reviews') }} r
  join {{ source('raw_olist', 'orders') }} o
    on r.order_id = o.order_id
  left join {{ source('raw_olist', 'order_items') }} oi
    on r.order_id = oi.order_id
    and SAFE_CAST(oi.order_item_id AS INT64) = 1  -- Assume first item if multiple per order
  where r.review_id is not null
),

with_sentiment as (
  select
    reviews.*,
    case
      when reviews.review_score >= 4 then 'positive'
      when reviews.review_score = 3 then 'neutral'
      else 'negative'
    end as review_sentiment
  from reviews
),

with_keys as (
  select
    {{ dbt_utils.generate_surrogate_key(['ws.review_id']) }} as review_key,
    ws.review_id,
    fo.order_id                  as order_key,
    dp.product_id                as product_key,
    dc.customer_unique_id        as customer_key,
    dd.date_key                  as review_date_key,
    ws.review_score,
    ws.review_sentiment,
    ws.review_comment_message is not null as has_comment
  from with_sentiment ws
  left join {{ ref('fct_orders') }} fo
    on ws.order_id = fo.order_id
  left join {{ ref('dim_products') }} dp
    on ws.product_id = dp.product_id
  left join {{ ref('dim_customers') }} dc
    on fo.customer_unique_id = dc.customer_unique_id
  left join {{ ref('dim_date') }} dd
    on CAST(FORMAT_DATE('%Y%m%d', DATE(ws.review_creation_date)) AS INT64) = dd.date_key
)

select
  review_key,
  review_id,
  order_key,
  product_key,
  customer_key,
  review_date_key,
  review_score,
  review_sentiment,
  has_comment
from with_keys
order by review_id
