with raw_teams as (
select * from {{ source('epl_duckdb', 'teams') }} QUALIFY row_number() over (partition by id, name order by pulse_id) = 1
)
select *, current_date as created_at,
{{ dbt_utils.generate_surrogate_key(['id', 'code','name','pulse_id']) }} as sid from raw_teams 