{{ config(materialized='ephemeral') }}

with source_data as (
    select
        to_varchar(order_id) as order_id,
        to_varchar(customer_id) as customer_id,
        to_varchar(order_date) as order_date,
        to_varchar(status) as order_status,
        to_varchar(product_id) as product_id,
        to_numeric(quantity) as quantity_ordered,
        to_numeric(price) as price,
        to_double(total_amount) as total_amount,
        to_timestamp(SUBSTR(cdc_timestamp,1,19), 'YYYY-MM-DD HH24:MI:SS') as change_time
    from {{ source('snowflake_source', 'orders') }}
)

select * from source_data
