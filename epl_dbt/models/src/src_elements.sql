

with raw_elements as (
select * from {{ source('epl_duckdb',  'elements') }}
     QUALIFY row_number() over 
            (partition by id, web_name, total_points , minutes  order by id, _dlt_list_idx desc) = 1
),
events as (
    select * from {{ source ('epl_duckdb', 'events') }} where is_current= 'True'
    )

select *,{{ dbt_utils.generate_surrogate_key(['re.id', 'team_code', 'second_name', 'web_name']) }} as sid,
current_date as created_at, erte.id as event from raw_elements  re join
                                             events erte on re._dlt_parent_id = erte._dlt_parent_id