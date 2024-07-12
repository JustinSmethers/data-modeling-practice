with renamed as (

    select
        id as product_id,
        name as product_name,
        category as product_category,
        price as product_price

    from {{ source('raw', 'products') }}

)

select *
from renamed