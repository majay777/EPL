
with raw_matches as (
select * from {{ source('epl_duckdb', 'matches') }}
  QUALIFY row_number() over (partition by code, event order by code, event) = 1
)

select *,current_date as created_at, case when month(kickoff_time) in (8,
                                     9,10,12,11) then year(kickoff_time):: varchar || '-' || (year(kickoff_time) + 1):: varchar when
                                     month(kickoff_time) in (1,2,3,4,5) then
                                     (year(kickoff_time) - 1):: varchar || '-' ||  year(kickoff_time):: varchar
                                     end as Season from raw_matches

