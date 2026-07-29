with raw_teams as (
    select 
        t.*,
        r.load_date
    from {{ source('epl_duckdb', 'teams') }} t
    left join {{ source('epl_duckdb', 'epl_raw_table') }} r
        on t._dlt_parent_id = r._dlt_id
),
teams_with_season as (
    select 
        *,
        {{ calculate_season('load_date') }} as Season
    from raw_teams
),
deduped as (
    select 
        *,
        current_date as created_at,
        {{ dbt_utils.generate_surrogate_key(['id', 'code', 'name', 'pulse_id', 'Season']) }} as sid
    from teams_with_season
    QUALIFY row_number() over (partition by id, name, Season order by pulse_id desc) = 1
)
select 
    * exclude (load_date)
from deduped