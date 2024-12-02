WITH reservation_data AS (
    SELECT 
        reservation_id,
        visitor_id,
        b.attraction_id,
        a.category as attraction_category,
        reservation_date_utc as reservation_date,
        visit_date,
        status,
        num_tickets,
        total_price,
        {{ dbt.datediff(
            'reservation_date_utc',
            'visit_date',
            'days'
        ) }} AS days_until_visit
    FROM {{ ref('fct_bookings') }} b
    LEFT JOIN {{ ref('dim_attractions') }} a
        on b.attraction_id = a.attraction_id
    WHERE status IN ('confirmed', 'pending', 'cancelled')
)

SELECT
    -- Métricas generales
    COUNT(*) AS total_reservations,  -- Total de reservas
    COUNT(CASE WHEN status = 'confirmed' THEN 1 END) AS confirmed_reservations,  -- Reservas confirmadas
    COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_reservations,  -- Reservas pendientes
    COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancelled_reservations,  -- Reservas canceladas

    -- Tasa de cancelación
    (COUNT(CASE WHEN status = 'cancelled' THEN 1 END) / NULLIF(COUNT(*), 0)) * 100 AS cancellation_rate,  -- Tasa de cancelación

    -- Ingresos por reservas
    SUM(CASE WHEN status = 'confirmed' THEN total_price ELSE 0 END) AS total_confirmed_revenue,  -- Ingresos por reservas confirmadas
    SUM(CASE WHEN status = 'cancelled' THEN total_price ELSE 0 END) AS total_cancelled_revenue,  -- Ingresos por reservas canceladas

    -- Promedio de días hasta la visita
    AVG(CASE WHEN status = 'confirmed' THEN days_until_visit ELSE NULL END) AS avg_days_until_visit_confirmed,  -- Promedio de días hasta la visita para reservas confirmadas
    AVG(CASE WHEN status = 'cancelled' THEN days_until_visit ELSE NULL END) AS avg_days_until_visit_cancelled,  -- Promedio de días hasta la visita para reservas canceladas

    -- Comparación de reservas canceladas por tipo de atracción
    COUNT(CASE WHEN status = 'cancelled' AND attraction_category = 'Simulador' THEN 1 END) AS Simulador_Cancelled,
    COUNT(CASE WHEN status = 'cancelled' AND attraction_category = 'Show' THEN 1 END) AS Show_Cancelled, 
    COUNT(CASE WHEN status = 'cancelled' AND attraction_category = 'Juego' THEN 1 END) AS Juego_Cancelled,
    COUNT(CASE WHEN status = 'cancelled' AND attraction_category = 'Tienda' THEN 1 END) AS Tienda_Cancelled,
    COUNT(CASE WHEN status = 'cancelled' AND attraction_category = 'Restaurante' THEN 1 END) AS Restaurante_Cancelled
FROM reservation_data 
