with base_visitors as (
    select * 
    from {{ ref('base_amusement_park_visitors') }}
)

select
    distinct membership_id,
    membership_type,
    benefits
from base_visitors