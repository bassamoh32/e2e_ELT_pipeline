{{ config(
    materialized='incremental',
    unique_key='OrderId',
    alias='orders'
) }}

with max_time as (
    -- Get the latest ChangeTime from the target table (only used in incremental runs)
    select coalesce(max(ChangeTime), to_timestamp('1900-01-01')) as last_run_time
    from {{ this }}
),

incremental_data as (
    select *
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where ChangeTime > (select last_run_time from max_time)
    {% endif %}
),

final as (
    select distinct
        OrderId,
        CustomerId,
        OrderDate,
        OrderStatus,
        ProductId,
        QuantityOrdered,
        Price,
        TotalAmount,
        ChangeTime,
        '{{ invocation_id }}' as batch_id
    from incremental_data
)

select * from final
