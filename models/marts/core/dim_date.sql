{{
  config(
    materialized='view'
  )
}}

WITH stg_date AS (
    SELECT * 
    FROM {{ ref("stg_date") }}
    ),

renamed_casted AS (
    SELECT *
    FROM stg_date
    )

SELECT * FROM renamed_casted