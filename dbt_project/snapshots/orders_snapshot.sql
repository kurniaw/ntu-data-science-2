{% snapshot orders_snapshot %}

{{
    config(
      target_database='ntu-data-science-ai',
      target_schema='snapshots',
      unique_key='order_id',
      strategy='timestamp',
      updated_at='order_purchase_timestamp'
    )
}}

select * from {{ ref('stg_orders') }}

{% endsnapshot %}
