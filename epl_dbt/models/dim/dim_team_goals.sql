with m as (select *
           from {{ ref("src_matches") }}),
     t as (select *
           from {{ ref("dim_teams") }}),
     
     -- Home goals scored
     home_goals_scored as (
         select m.team_h,
                t.name as club,
                t.id as team_id,
                m.team_h_score as goals_scored,
                0 as goals_conceded,
                true as is_home,
                m.Season
         from m
         join t on m.team_h = t.id and m.Season = t.Season
     ),
     
     -- Away goals scored
     away_goals_scored as (
         select m.team_a,
                t.name as club,
                t.id as team_id,
                m.team_a_score as goals_scored,
                0 as goals_conceded,
                false as is_home,
                m.Season
         from m
         join t on m.team_a = t.id and m.Season = t.Season
     ),
     
     -- Home goals conceded
     home_goals_conceded as (
         select m.team_h,
                t.name as club,
                t.id as team_id,
                0 as goals_scored,
                m.team_a_score as goals_conceded,
                true as is_home,
                m.Season
         from m
         join t on m.team_h = t.id and m.Season = t.Season
     ),
     
     -- Away goals conceded
     away_goals_conceded as (
         select m.team_a,
                t.name as club,
                t.id as team_id,
                0 as goals_scored,
                m.team_h_score as goals_conceded,
                false as is_home,
                m.Season
         from m
         join t on m.team_a = t.id and m.Season = t.Season
     ),
     
     -- Combine all
     all_goals as (
         select * from home_goals_scored
         union all
         select * from away_goals_scored
         union all
         select * from home_goals_conceded
         union all
         select * from away_goals_conceded
     )

select team_id,
       club,
       Season,
       is_home,
       sum(goals_scored) as goals_scored,
       sum(goals_conceded) as goals_conceded
from all_goals
group by team_id, club, Season, is_home
order by club, Season, is_home
