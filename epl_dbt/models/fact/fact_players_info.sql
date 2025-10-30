{{
    config(
        materialized= 'incremental',
        on_schema_change= 'fail',
        dist=('name','player_id','web_name', 'second_name', 'minutes')
    )
}}

with t as (
    select * from {{ ref("dim_teams") }}
),
m as (
    select * from {{ ref("src_matches") }}
),
el as (
    select * from (select *, row_number() over (partition by id, web_name order by id, "_dlt_list_idx" desc) as rnk from
               {{ ref ("src_elements") }} ) where rnk=1

),
ev as (
    select * from {{ ref("dim_events") }}
),
t1 as (
    select * from {{ ref("dim_teams") }}
)


(select  distinct(t.name) as OPP, {{ dbt_utils.generate_surrogate_key(['web_name', 'second_name', 't.name', 'ev.name']) }} as player_id,ev.name, el.web_name,el.second_name, t1.name as CLUB,  
          case when m.team_h_score- m.team_a_score = 0 then 'D' when m.team_h_score- m.team_a_score > 0 then 'L' 
          when m.team_h_score- m.team_a_score < 0 then 'W' end as result, el.event_points as Gameweek_Points,  el.total_points,
          el.starts, el.minutes, el.goals_scored, el.assists, el.expected_goals, 
          expected_assists,
          el.expected_goal_involvements, el.clean_sheets, el.goals_conceded, 
          el.expected_goals_conceded,el.own_goals,
          el.penalties_saved,el.penalties_missed,el.yellow_cards, el.red_cards, 
          el.saves, el.bonus, el.bps,el.influence,
          el.creativity, el.threat, el.ict_index, el.transfers_in_event - 
          el.transfers_out_event as Net_Transfer,
          el.transfers_in - el.transfers_out as Strength_bench, el.now_cost as COST, 
          case when month(m.kickoff_time) in (8, 
            9,10,12,11) then year(m.kickoff_time):: varchar || '-' || (year(m.kickoff_time) + 1):: varchar when 
            month(m.kickoff_time) in (1,2,3,4,5) then 
            (year(m.kickoff_time) - 1):: varchar || '-' ||  year(m.kickoff_time):: varchar
            end as Season
          from   m 
          join  el on m.team_a = el.team
          join  ev on m.event = ev.id 
          join  t on m.team_h = t.id
          join  t1 on m.team_a = t1.id) 
          union (select distinct(t.name) as OPP,{{ dbt_utils.generate_surrogate_key(['web_name', 'second_name', 't.name', 'ev.name']) }} as player_id, ev.name,  
          el.web_name,el.second_name, t1.name as club,case    when m.team_a_score - m.team_h_score = 0 then 'D' 
          when m.team_a_score - m.team_h_score > 0 then 'L' when m.team_a_score - m.team_h_score < 0 then 'W' end as result ,
          el.event_points as Gameweek_Points,  el.total_points, el.starts, el.minutes, el.goals_scored, el.assists, el.expected_goals, 
          expected_assists, 
          el.expected_goal_involvements, el.clean_sheets, el.goals_conceded, el.expected_goals_conceded, 
          el.own_goals, 
          el.penalties_saved,el.penalties_missed,el.yellow_cards, el.red_cards, el.saves, el.bonus, el.bps, 
          el.influence, 
          el.creativity, el.threat, el.ict_index, el.transfers_in_event - el.transfers_out_event as 
          Net_Transfer, 
          el.transfers_in - el.transfers_out as Strength_bench, el.now_cost as COST,
          case when month(m.kickoff_time) in (8, 
          9,10,12,11) then year(m.kickoff_time):: varchar || '-' || (year(m.kickoff_time) + 1):: varchar when 
          month(m.kickoff_time) in (1,2,3,4,5) then 
          (year(m.kickoff_time) - 1):: varchar || '-' ||  year(m.kickoff_time):: varchar
          end as Season
          from  m 
          join
           el on m.team_h = el.team
          join  ev on m.event = ev.id 
          join  t on m.team_a = t.id 
          join  t1 on m.team_h = t1.id) 