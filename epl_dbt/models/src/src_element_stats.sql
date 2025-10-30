

with raw_elements_stats as (
select * from {{ source('epl_duckdb', 'element_stats') }} QUALIFY row_number() over (partition by label, name order by label, name) = 1
)

select
  *, current_date as created_at, {{ dbt_utils.generate_surrogate_key(['label', 'name']) }} as sid from raw_elements_stats 
