WITH stg_membership AS (
    SELECT * 
    FROM {{ ref("stg_amusement_park__membership") }}
    )
SELECT * FROM stg_membership