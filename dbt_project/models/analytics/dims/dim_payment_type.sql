{{
  config(
    materialized='table',
    schema='analytics',
    tags=['dimension', 'static']
  )
}}

-- Payment type dimension: One row per payment method
-- Type 1 SCD - static reference data

with payment_types as (
  select distinct
    payment_type as payment_type_name
  from {{ source('raw_olist', 'order_payments') }}
  where payment_type is not null
),

enriched as (
  select 
    row_number() over (order by payment_type_name) as payment_type_key,
    payment_type_name,
    case 
      when payment_type_name in ('credit_card', 'debit_card') then 'online'
      when payment_type_name = 'boleto' then 'offline'
      else 'other'
    end as payment_channel,
    case 
      when payment_type_name = 'credit_card' then true
      else false
    end as requires_installments,
    case 
      when payment_type_name = 'credit_card' then 'medium'
      when payment_type_name = 'debit_card' then 'low'
      when payment_type_name = 'boleto' then 'high'
      else 'unknown'
    end as payment_risk_level
  from payment_types
)

select 
  payment_type_key,
  payment_type_name,
  payment_channel,
  requires_installments,
  payment_risk_level
from enriched
