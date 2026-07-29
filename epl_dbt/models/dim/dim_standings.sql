with dr as (select *
            from {{ ref("dim_results") }} QUALIFY row_number() over (partition by event_id, home_team_id, away_team_id order by kickoff_time) = 1
    ),
     t as (select * from {{ ref("dim_teams") }}),
    table1 as (
    PIVOT (
        select TEAM, Season, Result, sum(Result_count) as Result_count
        from (
            select home_team_id as TEAM, Season, count(Result_f_HOME) as Result_count, any_value(Result_f_Home) as Result
            from dr group by home_team_id, Season, Result_f_Home
            union all
            select away_team_id as TEAM, Season, count(Result_f_Away) as Result_count, any_value(Result_f_Away) as Result
            from dr group by away_team_id, Season, Result_f_Away
        )
        where TEAM is not null
        group by TEAM, Season, Result
    )
    ON Result USING sum(Result_count)
    GROUP BY TEAM, Season
    ),
    table2 as (
select *
from {{ ref ("dim_team_goals_scored_diffrence") }}

    )

select t.name as team,
       (coalesce(table1.W, 0) + coalesce(table1.D, 0) + coalesce(table1.L, 0)) as PLAYED,
       table1.W,
       table1.D,
       table1.L,
       table2.Goals_scored                                                     as GF,
       table2.Goals_conceded                                                   as GC,
       table2.Goal_difference                                                  as GD,
       ((coalesce(table1.W, 0) * 3) + (coalesce(table1.D, 0) * 1))             as Points,
       table1.Season
from table1 join table2 on table1.TEAM = table2.team_id and table1.Season = table2.Season
join t on table1.TEAM = t.id order by Points, GD desc
