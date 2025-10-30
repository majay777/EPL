


with raw_chip_plays as (
select * from {{ source('epl_duckdb', 'chip_plays') }}  QUALIFY row_number() over (partition by _dlt_id, chip_name, num_played , _dlt_parent_id ,_dlt_list_idx order by _dlt_id, chip_name, num_played , _dlt_parent_id ,_dlt_list_idx) = 1
)

select _dlt_id, chip_name, num_played , _dlt_parent_id ,_dlt_list_idx , current_date as created_at,
{{ dbt_utils.generate_surrogate_key(['_dlt_id', 'chip_name']) }} as sid from raw_chip_plays