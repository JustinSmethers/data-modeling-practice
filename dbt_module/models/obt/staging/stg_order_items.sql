with renamed as (

    select 
        id as order_items_id,
        order_id,
        product_id,
        quantity as order_quantity,
        price as order_price
    
    from {{ source('raw', 'order_items') }}

)

select *
from renamed