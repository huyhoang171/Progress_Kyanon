select
    md5(cast(customer_id as string)) as customer_sk,
    customer_id,
    customer_name,
    city
from {{ source('main_raw', 'raw_customers') }}