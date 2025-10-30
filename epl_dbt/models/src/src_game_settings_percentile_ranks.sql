
with raw_percentile_ranks as (
select * from {{ source('epl_duckdb', 'percentile_ranks')}}
          QUALIFY row_number() over (partition by value  order by value) = 1
)

select * from raw_percentile_ranks