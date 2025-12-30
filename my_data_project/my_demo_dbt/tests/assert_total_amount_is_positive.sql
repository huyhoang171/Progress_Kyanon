
select
    order_id,
    total_amount
from {{ ref('fact_sales') }}
where total_amount <= 0