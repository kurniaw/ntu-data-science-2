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
  left join {{ ref('product_category_name_translation') }} c
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
      when category_name_english in (
        'electronics', 'computers', 'computers_accessories',
        'telephony', 'fixed_telephony', 'tablets_printing_image',
        'audio', 'cine_photo', 'air_conditioning',
        'home_appliances', 'home_appliances_2',
        'small_appliances', 'small_appliances_home_oven_and_coffee'
      ) then 'Electronics'

      when category_name_english in (
        'perfumery', 'watches_gifts', 'luggage_accessories',
        'fashion_bags_accessories', 'fashion_shoes',
        'fashion_male_clothing', 'fashio_female_clothing',
        'fashion_underwear_beach', 'fashion_sport',
        'fashion_childrens_clothes'
      ) then 'Fashion'

      when category_name_english in (
        'bed_bath_table', 'housewares', 'furniture_decor',
        'furniture_living_room', 'furniture_bedroom',
        'furniture_mattress_and_upholstery', 'office_furniture',
        'kitchen_dining_laundry_garden_furniture',
        'home_confort', 'home_comfort_2', 'home_construction',
        'garden_tools', 'costruction_tools_garden',
        'construction_tools_construction', 'costruction_tools_tools',
        'construction_tools_lights', 'construction_tools_safety',
        'christmas_supplies', 'flowers'
      ) then 'Home'

      when category_name_english in (
        'health_beauty', 'sports_leisure', 'diapers_and_hygiene'
      ) then 'Health & Beauty'

      when category_name_english in (
        'art', 'arts_and_craftmanship', 'music',
        'musical_instruments', 'cds_dvds_musicals', 'dvds_blu_ray'
      ) then 'Art & Culture'

      when category_name_english in (
        'books_technical', 'books_general_interest', 'books_imported',
        'stationery'
      ) then 'Books & Media'

      when category_name_english in (
        'toys', 'baby', 'consoles_games', 'party_supplies'
      ) then 'Toys & Games'

      when category_name_english in (
        'auto'
      ) then 'Automotive'

      when category_name_english in (
        'food_drink', 'food', 'drinks', 'la_cuisine'
      ) then 'Food & Drink'

      -- Remaining → Other: cool_stuff, market_place, pet_shop,
      -- agro_industry_and_commerce, industry_commerce_and_business,
      -- signaling_and_security, security_and_services
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
