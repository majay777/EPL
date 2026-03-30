select
    player_natural_key,
    count(*) as current_cnt
from {{ ref('dim_players_scd2') }}
where is_current = true
group by player_natural_key
having count(*) != 1
