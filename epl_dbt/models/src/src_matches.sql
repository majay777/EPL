with raw_matches as (select *
                     from {{ source('epl_duckdb', 'matches') }}
                     where finished = True
    QUALIFY row_number() over (partition by code
   , event order by code
   , event) = 1
    )

select *,
       current_date as  created_at,
       {{ calculate_season('kickoff_time') }} as Season from raw_matches order by event,id