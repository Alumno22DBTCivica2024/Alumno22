WITH stg_visits AS (
    SELECT * 
    FROM {{ ref("stg_amusement_park__visits") }}
    )
SELECT * FROM stg_visits