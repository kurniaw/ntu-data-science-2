{{
  config(
    materialized='table',
    schema='analytics',
    tags=['dimension', 'weekly'],
    unique_id='zip_code_prefix,state'
  )
}}

-- Geographic dimension: One row per zip code prefix
-- Source: geolocation table
-- Type 1 SCD - static data, never changes

with geo as (
  select distinct
    row_number() over (order by geolocation_zip_code_prefix, geolocation_state) as location_key,
    geolocation_zip_code_prefix as zip_code_prefix,
    geolocation_city as city,
    geolocation_state as state,
    geolocation_lat as latitude,
    geolocation_lng as longitude
  from {{ source('raw_olist', 'geolocation') }}
  where geolocation_zip_code_prefix is not null
),

enriched as (
  select 
    location_key,
    zip_code_prefix,
    city,
    state,
    latitude,
    longitude,
    -- Derive region from state
    case 
      when state in ('AC', 'AM', 'AP', 'PA', 'RO', 'RR') then 'North'
      when state in ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') then 'Northeast'
      when state in ('DF', 'GO', 'MS', 'MT') then 'West'
      when state in ('ES', 'MG', 'RJ', 'SP') then 'Southeast'
      when state in ('PR', 'RS', 'SC') then 'South'
      else 'Unknown'
    end as region
  from geo
)

select 
  location_key,
  zip_code_prefix,
  city,
  state,
  latitude,
  longitude,
  region,
  CAST(NULL AS STRING) as regional_sales_tier  -- TODO: Populate based on aggregated sales
from enriched
order by state, city
