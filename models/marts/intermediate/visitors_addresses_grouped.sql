WITH stg_visitors AS (
    SELECT 
        visitor_id,
        username,
        first_name,
        last_name,
        membership_id,
        address_id
    FROM {{ ref('stg_amusement_park__visitors') }}
),

stg_addresses AS (
    SELECT 
        address_id,
        state,
        zipcode,
        address,
        country
    FROM {{ ref('stg_amusement_park__addresses') }}
)

SELECT 
    u.visitor_id,
    u.username,
    u.first_name,
    u.last_name,
    u.membership_id,
    a.address_id,
    a.state,
    a.zipcode,
    a.address,
    a.country
FROM stg_visitors u
LEFT JOIN stg_addresses a
    ON u.address_id = a.address_id
