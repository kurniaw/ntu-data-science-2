{{
  config(
    materialized='table',
    schema='analytics',
    tags=['dimension', 'daily']
  )
}}

-- Calendar dimension table spanning 5 years back and 5 years forward
-- Updated: One row per calendar day
-- This is a Type 1 SCD (static, never changes)

with dates as (
  select
    CAST(FORMAT_DATE('%Y%m%d', calendar_date) AS INT64) as date_key,
    calendar_date,
    EXTRACT(YEAR FROM calendar_date) as year,
    EXTRACT(QUARTER FROM calendar_date) as quarter,
    EXTRACT(MONTH FROM calendar_date) as month,
    EXTRACT(DAY FROM calendar_date) as day_of_month,
    MOD(EXTRACT(DAYOFWEEK FROM calendar_date) + 5, 7) + 1 as day_of_week, -- 1=Monday, 7=Sunday
    EXTRACT(ISOWEEK FROM calendar_date) as week_of_year,
    case
      when EXTRACT(DAYOFWEEK FROM calendar_date) in (1, 7) then true
      else false
    end as is_weekend,
    FORMAT_DATE('%A', calendar_date) as day_name,
    FORMAT_DATE('%B', calendar_date) as month_name,
    false as is_holiday  -- TODO: Populate Brazil holidays
  from unnest(
    GENERATE_DATE_ARRAY(
      DATE_SUB(CURRENT_DATE, INTERVAL 5 YEAR),
      DATE_ADD(CURRENT_DATE, INTERVAL 5 YEAR)
    )
  ) as calendar_date
)

select
  date_key,
  calendar_date,
  year,
  quarter,
  month,
  day_of_month,
  day_of_week,
  week_of_year,
  day_name,
  month_name,
  is_weekend,
  is_holiday,
  DATE_DIFF(calendar_date, DATE '1970-01-01', DAY) as days_since_epoch
from dates
order by calendar_date
