WITH reservation_data AS (
    -- Extraemos los datos de la tabla de reservas
    SELECT 
        reservation_id,
        visitor_id,
        attraction_id,
        reservation_date_utc as reservation_date,
        visit_date,
        {{ dbt.datediff(
            'reservation_date_utc',
            'visit_date',
            'days'
        ) }} AS days_until_visit,
        num_tickets,
        total_price
    FROM {{ ref('fct_bookings') }}
    WHERE status = 'confirmed'
),

summary AS (
    SELECT
        COUNT(*) AS total_reservations,  -- Total de reservas
        COUNT(CASE WHEN days_until_visit <= 30 THEN 1 END) AS last_minute_reservations,  -- Reservas de último minuto (<= 30 días)
        COUNT(CASE WHEN days_until_visit > 30 THEN 1 END) AS early_reservations,  -- Reservas anticipadas (> 30 días)
        AVG(days_until_visit) AS avg_days_until_visit,  -- Promedio de días entre reserva y visita
        SUM(CASE WHEN days_until_visit <= 30 THEN total_price ELSE 0 END) AS total_last_minute_revenue,  -- Ingresos de reservas de último minuto
        SUM(CASE WHEN days_until_visit > 30 THEN total_price ELSE 0 END) AS total_early_revenue  -- Ingresos de reservas anticipadas
    FROM reservation_data
)

SELECT 
    total_reservations,
    last_minute_reservations,
    early_reservations,
    avg_days_until_visit,
    ROUND(total_last_minute_revenue,2) as total_last_minute_revenue,
    ROUND(total_early_revenue,2) AS total_early_revenue,
    (last_minute_reservations / NULLIF(total_reservations, 0)) * 100 AS pct_last_minute_reservations,  -- % de reservas de último minuto
    (early_reservations / NULLIF(total_reservations, 0)) * 100 AS pct_early_reservations  -- % de reservas anticipadas
FROM summary
