WITH base_visitors AS (
    SELECT * 
    FROM {{ ref('base_amusement_park_visitors') }}
    )

SELECT
    visitor_id,
    username,
    first_name,
    last_name,
    signup_date_utc,
    membership_id,
    address_id
FROM base_visitors