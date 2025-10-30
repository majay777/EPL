

with raw_epl_raw_table as (
select * from {{ source('epl_duckdb', 'epl_raw_table') }}
)

select * from raw_epl_raw_table