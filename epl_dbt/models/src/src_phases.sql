

with raw_phases as (
select * from {{ source('epl_duckdb', 'phases')}} QUALIFY row_number() over (partition by id, name order by name,_dlt_id) = 1
)
select *,{{ dbt_utils.generate_surrogate_key(['id', 'name','start_event','stop_event']) }} as sid  from raw_phases