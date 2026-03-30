select *
from {{ ref('dim_players_scd2') }}
where valid_to is not null
  and is_current = true
