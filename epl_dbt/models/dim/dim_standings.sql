--with dr as (
--    select * from {{ ref("dim_results") }} QUALIFY row_number() over (partition by name, HOME_TEAM, AWAY_TEAM order by kickoff_time) = 1
--
--),
--table1 as (
--    PIVOT (select HOME_TEAM as TEAM,Result, sum(Result_count) as Result_count, sum(HOME_GOALS) as
--                GOALS_FIRED,
--                sum(AWAy_GOAls) as GOALS_CONCEDED, GOALS_FIRED - GOALS_CONCEDED as GOAL_DIFFERENCE from (
--                (select HOME_TEAM,
--                count(Result_f_HOME) as Result_count, any_value(Result_f_Home) as Result,
--                sum(HOME_TEAM_GOALS) as HOME_GOALS, sum(AWAY_TEAM_GOALS) as AWAY_GOALS
--                from dr group by HOME_TEAM, Result_f_Home) union (select AWAY_TEAM, count(Result_f_Away) as
--                Result_count, any_value(Result_f_Away) as Result, sum(AWAY_TEAM_GOALS) as AWAY_GOALS,
--                sum(HOME_TEAM_GOALS) as HOME_GOALS
--                from dr group by AWAY_TEAM, Result_f_Away)) where Team != '' group by TEAM,Result order by
--                TEAM, Result) on
--               Result Using sum(Result_count) group by Team
--),
--table2 as (
--    select * from {{ ref("dim_team_goals_scored_diffrence") }}
--
--)
--
--select table1.team, (NULLIF(table1.W,0) + NULLIF (table1.D,0) + 	NULLIF(table1.L, 0) ) as PLAYED, table1.W, table1.D, table1.L, table2.Goals_scored as GF, table2.Goals_conceded as GC,
--    table2.Goal_difference as GD,
--    ((table1.W * 3) + (table1.D * 1)) as Points, case when month(current_date) in (8,
--                9,10,12,11) then year(current_date):: varchar || '-' || (year(current_date) + 1):: varchar when
--                month(current_date) in (1,2,3,4,5) then
--                (year(current_date) - 1):: varchar || '-' ||  year(current_date):: varchar
--                end as Season from table1 join table2 on table1.team = table2.club

with dr as (
        select * from {{ ref("dim_results") }} QUALIFY row_number() over (partition by name, HOME_TEAM, AWAY_TEAM order by kickoff_time) = 1

),

table1 as (
    PIVOT (select HOME_TEAM as TEAM,Result, sum(Result_count) as Result_count, sum(HOME_GOALS) as
GOALS_FIRED,
sum(AWAy_GOAls) as GOALS_CONCEDED, GOALS_FIRED - GOALS_CONCEDED as GOAL_DIFFERENCE from (
    (select HOME_TEAM,
    count(Result_f_HOME) as Result_count, any_value(Result_f_Home) as Result,
sum(HOME_TEAM_GOALS) as HOME_GOALS, sum(AWAY_TEAM_GOALS) as AWAY_GOALS
FROM  dr group by HOME_TEAM, Result_f_Home) union all(select AWAY_TEAM, count(Result_f_Away) as
Result_count, any_value(Result_f_Away) as Result, sum(AWAY_TEAM_GOALS) as AWAY_GOALS,
sum(HOME_TEAM_GOALS) as HOME_GOALS
FROM  dr group by AWAY_TEAM, Result_f_Away)) where Team != '' group by TEAM,Result order by
TEAM, Result) on
Result Using sum(Result_count) group by Team
),
table2 as (
        select * from {{ ref("dim_team_goals_scored_diffrence") }}

)

select table1.team, (coalesce(table1.W,0) + coalesce (table1.D,0) + 	coalesce(table1.L, 0) )as PLAYED, table1.W, table1.D, table1.L, table2.Goals_scored as GF, table2.Goals_conceded as GC,
table2.Goal_difference as GD,
((coalesce(table1.W,0) * 3) + (coalesce (table1.D,0) * 1)) as Points, case when month(current_date) in (8,
                                                                                                    9,10,12,11) then year(current_date):: varchar || '-' || (year(current_date) + 1):: varchar when
month(current_date) in (1,2,3,4,5) then
(year(current_date) - 1):: varchar || '-' ||  year(current_date):: varchar
end as Season from table1 join table2 on table1.team = table2.club order by Points, GD desc
