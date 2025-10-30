with m as (
    select * from {{ ref("src_matches") }}
),
t1 as (
    select * from {{ ref ("dim_teams") }}

)

select any_value(distinct(t1.name)) as club,  sum(m.team_h_score) as goals_scored, case when month(any_value(m.kickoff_time)) in (8, 
            9,10,12,11) then year(any_value(m.kickoff_time)):: varchar || '-' || (year(any_value(m.kickoff_time)) + 1):: varchar when 
            month(any_value(m.kickoff_time)) in (1,2,3,4,5) then 
            (year(any_value(m.kickoff_time)) - 1):: varchar || '-' ||  year(any_value(m.kickoff_time)):: varchar
            end as Season from 
                m
                 join  t1 on m.team_h = t1.id group by m.team_h order by goals_scored desc