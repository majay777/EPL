with raw_tiebreak_stats as (
select * from {{ source('epl_duckdb', 'league_h2h_tiebreak_stats')}} QUALIFY row_number() over (partition by _dlt_id order by _dlt_id) = 1
)
select * from raw_tiebreak_stats
