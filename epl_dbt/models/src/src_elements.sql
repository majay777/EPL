

with raw_elements as (
select * from {{ source('epl_duckdb',  'elements') }}
     QUALIFY row_number() over 
            (partition by id, web_name, total_points , minutes  order by id, _dlt_list_idx desc) = 1
)

select *,{{ dbt_utils.generate_surrogate_key(['id', 'team_code', 'second_name', 'web_name']) }} as sid,
current_date as created_at from raw_elements 