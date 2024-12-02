WITH stg_attractions AS (
    SELECT * 
    FROM {{ ref("stg_amusement_park__attractions") }}
    )
    
SELECT * FROM stg_attractions