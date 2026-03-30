with versions as (
    select
        player_natural_key,
        club,
        cost,
        minutes,
        starts,
        count(*) as cnt
    from {{ ref('dim_players_scd2') }}
    group by
        player_natural_key,
        club,
        cost,
        minutes,
        starts
)

select *
from versions
where cnt > 1
