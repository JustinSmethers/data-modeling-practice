with renamed as (

    select
        id as order_id,
        customer_id,
        order_date,
        total_amount as total_order_amount
    
    from {{ source('raw', 'orders') }}

)

select *
from renamed