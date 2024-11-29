WITH stg_address AS (
    SELECT * 
    FROM {{ ref("stg_amusement_park__addresses") }}
    )

SELECT * FROM stg_address