with orders as (
    select * from {{ ref('stg_orders') }}
),
products as (
    select * from {{ source('main_raw', 'raw_products') }}
)

select
    o.order_id,
    md5(cast(o.customer_id as string)) as customer_sk, -- Surrogate Key from dim_customers
    o.product_id,
    o.order_date,
    o.quantity,
    o.quantity * p.price as total_amount
from orders o
join products p on o.product_id = p.product_id