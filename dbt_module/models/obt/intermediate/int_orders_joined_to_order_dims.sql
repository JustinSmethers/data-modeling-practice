with orders as (

    select * from {{ ref('stg_orders') }}

), order_items as (

    select * from {{ ref('stg_order_items') }}

), customers as (

    select * from {{ ref('stg_customers') }}

), products as (

    select * from {{ ref('stg_products') }}

)

select
    -- orders
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_order_amount,

    -- order_items
    oi.order_items_id,
    oi.product_id,
    oi.order_quantity,
    oi.order_price,
    
    -- customers
    c.customer_name,
    c.customer_email,
    c.customer_account_created_at,
    
    -- products
    p.product_name,
    p.product_category,
    p.product_price

from orders o

join order_items oi
    on o.order_id = oi.order_id
join customers c
    on o.customer_id = c.customer_id
join products p
    on oi.product_id = p.product_id
    