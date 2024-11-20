WITH product_contributions AS (
    SELECT 
        oi.order_id
        ,oi.product_id
        ,oi.quantity
        ,p.price
        ,(oi.quantity * p.price) AS product_cost
        ,SUM(oi.quantity * p.price) OVER (PARTITION BY oi.order_id) AS total_products_cost
        ,o.order_cost
        ,o.shipping_cost
    FROM 
        {{ ref('stg_sql_server_dbo__order_items') }} oi
    LEFT JOIN 
        {{ ref('stg_sql_server_dbo__products') }} p ON oi.product_id = p.product_id
    LEFT JOIN 
        {{ ref('stg_sql_server_dbo__orders') }} o ON oi.order_id = o.order_id
),
distributed_costs AS (
    SELECT 
        order_id,
        product_id,
        quantity,
        product_cost,
        product_cost / total_products_cost AS cost_ratio,
        product_cost / total_products_cost * order_cost AS order_cost_per_product,
        product_cost / total_products_cost * shipping_cost AS shipping_cost_per_product
    FROM product_contributions
)
SELECT 
    order_id,
    product_id,
    quantity,
    product_cost,
    order_cost_per_product,
    shipping_cost_per_product,
    product_cost + shipping_cost_per_product AS total_cost_per_product
FROM distributed_costs
