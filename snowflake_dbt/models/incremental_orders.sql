{{ 
    config(
        materialized='incremental',
        unique_key='order_id',
        alias='orders'
    )
}}

{% if is_incremental() %}
with max_time as (
    select coalesce(max(change_time), '1900-01-01') as last_time
    from {{ this }}
),
incremental_data as (
    select *
    from {{ ref('stg_orders') }} s
    join max_time m on 1=1
    where s.change_time > m.last_time
)
{% else %}
with incremental_data as (
    select *
    from {{ ref('stg_orders') }}
)
{% endif %}

, final as (
    select distinct
        order_id,
        customer_id,
        order_date,
        order_status,
        product_id,
        quantity_ordered,
        price,
        total_amount,
        change_time,
        '{{ invocation_id }}' as batch_id
    from incremental_data
)

select * from final