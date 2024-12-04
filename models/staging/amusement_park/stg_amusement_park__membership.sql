with base_visitors as (
    select * 
    from {{ ref('base_amusement_park_visitors') }}
)

select
    distinct membership_id,
    membership_type,
    CASE 
        WHEN benefits IS NULL OR benefits = 'NaN' THEN 0
        ELSE benefits
    END AS benefits
from base_visitors