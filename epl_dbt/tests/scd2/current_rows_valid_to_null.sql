select *
from {{ ref('dim_players_scd2') }}
where is_current = true
  and valid_to is not null
