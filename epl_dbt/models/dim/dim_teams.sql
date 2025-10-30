with teams as
(
select * from {{ ref("src_teams") }}
),
e as (
select * from {{ ref("src_events")}}

),
dt as (
select *, case when month(e.deadline_time) in (8,
                         9,10,12,11) then year(e.deadline_time):: varchar || '-' || (year(e.deadline_time) + 1):: varchar when
                         month(e.deadline_time) in (1,2,3,4,5) then
                          (year(e.deadline_time) - 1):: varchar || '-' ||  year(e.deadline_time):: varchar
                          end as Season, from teams, e
)

select  distinct(sid) as sid ,name, short_name, code,  id,
                        strength,  strength_overall_home, strength_overall_away, strength_attack_home,
                       strength_attack_away, strength_defence_home, strength_defence_away, pulse_id, season
                       from dt