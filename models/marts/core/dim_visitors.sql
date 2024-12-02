WITH stg_visitors AS (
    SELECT * 
    FROM {{ ref("stg_amusement_park__visitors") }}
    )
SELECT * FROM stg_visitors