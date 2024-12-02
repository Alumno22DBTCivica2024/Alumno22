WITH stg_budget AS (
    SELECT * 
    FROM {{ ref("stg_amusement_park__budget") }}
    ),

renamed_casted AS (
    SELECT *
    FROM stg_budget
    )

SELECT * FROM renamed_casted