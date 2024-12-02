WITH bookings AS (
    SELECT 
        reservation_id,
        attraction_id,
        num_tickets,
        total_price,
        visit_date
    FROM {{ ref('fct_bookings') }}
    WHERE status = 'confirmed'
),

transactions AS (
    SELECT 
        transaction_id,
        attraction_id,
        price AS transaction_total_price,
        transaction_time
    FROM {{ ref('fct_transactions') }}
),

combined_data AS (
    -- Unificamos las visitas de las reservas y las transacciones en un solo conjunto
    SELECT 
        attraction_id,
        num_tickets AS entries,
        total_price AS revenue,
        'Booking' AS source_type
    FROM bookings
    UNION ALL
    SELECT 
        attraction_id,
        1 AS entries,  -- Cada transacción cuenta como una visita única
        transaction_total_price AS revenue,
        'Transaction' AS source_type
    FROM transactions
),

attraction_summary AS (
    -- Agrupamos los datos por atracción para obtener las métricas relevantes
    SELECT
        attraction_id,
        SUM(entries) AS total_visits,  -- Total de visitas (por reserva o transacción)
        SUM(revenue) AS total_revenue,  -- Total de ingresos por atracción
        SUM(CASE WHEN source_type = 'Booking' THEN entries ELSE 0 END) AS total_bookings,  -- Total de reservas
        SUM(CASE WHEN source_type = 'Transaction' THEN 1 ELSE 0 END) AS total_transactions  -- Total de transacciones
    FROM combined_data
    GROUP BY attraction_id
)

SELECT 
    a.attraction_id,
    at.name AS attraction_name,
    a.total_visits,
    a.total_revenue,
    a.total_bookings,
    a.total_transactions
FROM attraction_summary a
JOIN {{ ref('dim_attractions') }} at ON a.attraction_id = at.attraction_id
ORDER BY a.total_visits DESC
