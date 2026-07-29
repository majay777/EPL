with m as (select *
           from {{ ref("src_matches") }}),
     et as (select *
            from {{ ref("dim_events") }}
            where true),
     t1 as (select *
            from {{ ref("dim_teams") }}
            where true),
     t as (select *
           from {{ ref("dim_teams") }}
            where true)

select distinct(t.id)                      as away_team_id,
               et.id as event_id,
               m.team_h_score                as HOME_TEAM_GOALS,
               m.team_a_score                as AWAY_TEAM_GOALS,
               (team_a_score - team_h_score) as GD,
               t1.id                       as home_team_id,
               m.Season,
                case 
                when GD > 0 then  3
                  when GD = 0 then  1
                  when GD < 0 then  0
end as Points_f_Away,
                  case 
                  when GD > 0 then 'W'
                  when GD = 0 then 'D'
               when GD < 0 then 'L'
end as Result_f_Away,  case when GD > 0 then  'L'
                when GD = 0 then 'D'
               when GD < 0 then 'W'
end as Result_f_Home,case when GD > 0 then 0 when GD = 0 then 1 when GD <0 then 3
end
as Points_f_Home, m.kickoff_time from m  join  t on m.team_a
                = t.id and m.Season = t.Season
                join  t1 on m.team_h = t1.id and m.Season = t1.Season
                join  et on m.event = et.id and m.Season = et.Season where m.finished
