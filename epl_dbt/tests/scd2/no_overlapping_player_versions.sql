with ordered as (
    select
        player_natural_key,
        valid_from,
        coalesce(valid_to, '9999-12-31') as valid_to,
        lag(valid_from) over (
            partition by player_natural_key
            order by valid_from
        ) as prev_valid_from,
        lag(coalesce(valid_to, '9999-12-31')) over (
            partition by player_natural_key
            order by valid_from
        ) as prev_valid_to
    from {{ ref('dim_players_scd2') }}
)

select *
from ordered
where prev_valid_to > valid_from
