with renamed as (

    select
        id as customer_id,
        name as customer_name,
        email as customer_email,
        created_at as customer_account_created_at
    
    from {{ source('raw', 'customers') }}

)

select *
from renamed