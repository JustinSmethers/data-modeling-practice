select
    order_id,
    customer_id,
    order_date,
    array_agg(order_items_id) as order_items,
    array_agg(product_id) as products,
    array_agg(order_quantity) as order_quantities,
    array_agg(order_price) as order_prices,
    sum(order_quantity * order_price) as total_order_amount

from {{ ref('int_orders_joined_to_order_items') }}

group by
    order_id,
    customer_id,
    order_date