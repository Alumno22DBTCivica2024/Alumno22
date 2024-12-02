with base_visitors as (
    select * 
    from {{ ref('base_amusement_park_visitors') }}
)

select
    distinct membership_id,
    membership_type,
    COALESCE(benefits, '0') AS benefits
from base_visitors