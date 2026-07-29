{{ config(
    materialized='incremental',
    unique_key='player_sk',
     on_schema_change='sync_all_columns'
) }}

with src as (

    select
        el.id              as player_natural_key,
        el.web_name,
        el.second_name,
        t.name             as club,
        el.now_cost        as cost,
        el.minutes,
        el.starts,
        current_timestamp  as valid_from,
        el.Season
    from {{ ref('src_elements') }} el
    join {{ ref('dim_teams') }} t
        on el.team = t.id and el.Season = t.Season
),

latest as (

    select *,
           row_number() over (
               partition by player_natural_key, Season
               order by valid_from desc
           ) as rn
    from src
),

current_snapshot as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'player_natural_key',
            'valid_from',
            'Season'
        ]) }}                   as player_sk,
        player_natural_key,
        web_name,
        second_name,
        club,
        cost,
        minutes,
        starts,
        valid_from,
        cast(null as timestamp) as valid_to,
        true                    as is_current,
        Season
    from latest
    where rn = 1
)

{% if is_incremental() %}

, changed_records as (

    select
        s.*
    from current_snapshot s
    join {{ this }} d
      on s.player_natural_key = d.player_natural_key
     and s.Season = d.Season
     and d.is_current = true
    where
          s.club    <> d.club
       or s.cost    <> d.cost
       or s.minutes <> d.minutes
       or s.starts  <> d.starts
),

expired_records as (

    select
        d.player_sk,
        d.player_natural_key,
        d.web_name,
        d.second_name,
        d.club,
        d.cost,
        d.minutes,
        d.starts,
        d.valid_from,
        current_timestamp as valid_to,
        false             as is_current,
        d.Season
    from {{ this }} d
    join changed_records c
      on d.player_natural_key = c.player_natural_key
     and d.Season = c.Season
     and d.is_current = true
)

{% endif %}

select *
from current_snapshot {% if is_incremental() %}
union all
select *
from expired_records {% endif %}
