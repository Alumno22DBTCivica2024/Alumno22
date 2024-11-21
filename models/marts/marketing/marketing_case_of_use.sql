WITH user_purchase_details AS (
    SELECT 
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        COUNT(o.order_id) AS total_orders,
        SUM(os.order_cost) AS total_spent,
        SUM(os.shipping_cost) AS total_shipping_cost,
        SUM(promo.discount) AS total_discount,
        SUM(oi.quantity) AS total_products_bought,
        COUNT(DISTINCT oi.product_id) AS total_different_products_bought 
    FROM {{ ref('stg_sql_server_dbo__users') }} AS u
    JOIN {{ ref('stg_sql_server_dbo__orders_reduced') }} AS o ON u.user_id = o.user_id  -- Tabla pedidos
    LEFT JOIN {{ ref('stg_sql_server_dbo__orders_bridge') }} AS os ON o.order_id = os.order_id  -- Tabla puente
    LEFT JOIN {{ ref('stg_sql_server_dbo__order_items') }} AS oi ON o.order_id = oi.order_id  -- Tabla de prods por pedido
    LEFT JOIN {{ ref('stg_sql_server_dbo__promos') }} AS promo ON os.promo_id = promo.promo_id --Tabla de promos
    GROUP BY u.user_id, u.first_name, u.last_name, u.email
)

SELECT 
    upd.user_id,
    upd.first_name,
    upd.last_name,
    upd.email,
    upd.total_orders,
    upd.total_spent,
    upd.total_shipping_cost,
    upd.total_discount,
    upd.total_products_bought,
    upd.total_different_products_bought
FROM user_purchase_details upd
ORDER BY upd.total_spent DESC
