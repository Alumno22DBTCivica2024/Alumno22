WITH reservation_data AS (
    -- Extraemos los datos de las reservas confirmadas
    SELECT 
        reservation_id,
        attraction_id,
        total_price,
        EXTRACT(YEAR FROM reservation_date_utc) AS reservation_year,
        EXTRACT(MONTH FROM reservation_date_utc) AS reservation_month,
        CONCAT(EXTRACT(YEAR FROM reservation_date_utc), '-', LPAD(EXTRACT(MONTH FROM reservation_date_utc), 2, '0')) AS month
    FROM {{ ref('fct_bookings') }}
    WHERE status = 'confirmed'
),
transaction_data AS (
    -- Extraemos los datos de las transacciones
    SELECT 
        transaction_id,
        attraction_id,
        price,
        EXTRACT(YEAR FROM transaction_time) AS transaction_year,
        EXTRACT(MONTH FROM transaction_time) AS transaction_month,
        CONCAT(EXTRACT(YEAR FROM transaction_time), '-', LPAD(EXTRACT(MONTH FROM transaction_time), 2, '0')) AS month
    FROM {{ ref('fct_transactions') }}
),
combined_data AS (
    -- Combinamos transacciones y reservas por atracción y mes
    SELECT
        attraction_id,
        month,
        SUM(total_price) AS monthly_revenue  -- Ingreso mensual por atracción (transacciones + reservas)
    FROM (
        SELECT attraction_id, month, total_price FROM reservation_data
        UNION ALL
        SELECT attraction_id, month, price FROM transaction_data
    ) AS all_revenue
    GROUP BY attraction_id, month
)

SELECT 
    b.attraction_id,
    b.month,
    b.rounded_target_revenue as target_revenue,  -- Ingresos previstos según el presupuesto
    b.target_visits,   -- Visitas previstas según el presupuesto
    COALESCE(c.monthly_revenue, 0) AS actual_revenue,  -- Ingresos reales según las reservas y transacciones
    COALESCE(c.monthly_revenue, 0) - b.rounded_target_revenue AS revenue_difference,  -- Diferencia de ingresos (real - presupuesto)
    CASE
        WHEN COALESCE(c.monthly_revenue, 0) >= b.rounded_target_revenue THEN 'Met'
        ELSE 'Not Met'
    END AS revenue_status  -- Estado del objetivo de ingresos
FROM {{ ref('monthly_budget') }} b
LEFT JOIN combined_data c
    ON b.attraction_id = c.attraction_id
    AND b.month = c.month
ORDER BY b.attraction_id, b.month
