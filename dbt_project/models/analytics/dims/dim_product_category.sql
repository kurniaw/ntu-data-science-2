{{
  config(
    materialized='table',
    schema='analytics',
    tags=['dimension', 'weekly'],
    unique_id='category_name,valid_from_date'
  )
}}

-- Product category dimension: One row per product category
-- Type 2 SCD - tracks historical changes
-- Current version has is_current = true and valid_to_date = NULL

with category_source as (
  select 
    p.product_category_name as category_name,
    c.category_name_english
  from {{ source('raw_olist', 'products') }} p
  left join {{ source('raw_olist', 'category_name_translate') }} c 
    on p.product_category_name = c.category_name
  where p.product_category_name is not null
),

deduplicated as (
  select distinct
    category_name,
    category_name_english
  from category_source
),

enriched as (
  select 
    row_number() over (order by category_name_english) as category_key,
    category_name,
    category_name_english,
    case 
      when category_name_english like '%electronics%' or category_name_english like '%computer%' then 'Electronics'
      when category_name_english like '%fashion%' or category_name_english like '%clothing%' then 'Fashion'
      when category_name_english like '%home%' or category_name_english like '%furniture%' then 'Home'
      when category_name_english like '%health%' or category_name_english like '%beauty%' then 'Health & Beauty'
      else 'Other'
    end as category_type,
    CAST(CURRENT_DATE AS TIMESTAMP) as valid_from_date,
    CAST(NULL AS TIMESTAMP) as valid_to_date,
    true as is_current
  from deduplicated
)

select 
  category_key,
  category_name,
  category_name_english,
  category_type,
  valid_from_date,
  valid_to_date,
  is_current
from enriched
order by category_name_english
