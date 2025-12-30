select
    cast(order_id as int) as order_id,
    cast(customer_id as int) as customer_id,
    product_id,
    cast(order_date as date) as order_date,
    cast(quantity as int) as quantity
from {{ source('main_raw', 'raw_orders') }}