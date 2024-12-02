WITH bookings AS (
    SELECT 
        reservation_id,
        num_tickets,
        (total_price/num_tickets) AS booking_total_price,
        visit_date,
        status
    FROM {{ ref('fct_bookings') }}
    WHERE status = 'confirmed'  -- Solo consideramos las reservas confirmadas
),

transactions AS (
    SELECT 
        transaction_id,
        price AS transaction_total_price,
        transaction_time
    FROM {{ ref('fct_transactions') }}
),

combined_data AS (
    SELECT 
        'Booking' AS source_type,
        reservation_id AS id,
        num_tickets AS total_entries,
        booking_total_price AS total_price,
        visit_date AS transaction_date
    FROM bookings
    UNION ALL
    SELECT 
        'Transaction' AS source_type,
        transaction_id AS id,
        1 AS total_entries,
        transaction_total_price AS total_price,
        transaction_time AS transaction_date
    FROM transactions
)

SELECT 
    source_type,  -- (Reserva o Transacción)
    SUM(total_entries) AS total_entries,
    SUM(total_price) AS total_revenue,
    AVG(total_price) AS average_revenue_per_entry
FROM combined_data
GROUP BY source_type
