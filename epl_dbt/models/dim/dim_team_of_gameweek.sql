with
el as (  select * from (select *, row_number() over (partition by id, web_name order by id, "_dlt_list_idx" desc) as rnk from
                   {{ ref ("src_elements") }})  where rnk=1
),
t as ( select * from {{ ref("dim_teams") }}

),
elt as (select * from {{ ref("src_element_types") }} 

 
),
m as (select * from {{ ref("src_matches")}}


)

(select  distinct(elt.pos_name) as POS, el.web_name as PLAYER_NAME,  t.name as club, 
            event_points as Points, (Select  m.event  from {{ ref ("src_matches") }} m where m.finished = 'true' order by m.code desc limit 1) as gameweek,
            
            (select max(Season) as Season  from (select m.kickoff_time, case when month(m.kickoff_time) in (8, 
            9,10,12,11) then year(m.kickoff_time):: varchar || '-' || (year(m.kickoff_time) + 1):: varchar when 
            month(m.kickoff_time) in (1,2,3,4,5) then 
            (year(m.kickoff_time) - 1):: varchar || '-' ||  year(m.kickoff_time):: varchar end as Season from src_matches m)) as Season 
            from  el 
            join  t on el.team = t.id
            join  elt on el.element_type = elt.id 
            where elt.pos_name = 'Goalkeeper' order by event_points desc, now_cost asc  limit 1)
            union 
            (select distinct(elt.pos_name) as POS, el.web_name as PLAYER_NAME,  t.name as club, 
            event_points as Points,(Select  m.event  from m where m.finished = 'true' order by m.code desc limit 1) as gameweek,
            (select max(Season) as Season  from (select m.kickoff_time, case when month(m.kickoff_time) in (8, 
            9,10,12,11) then year(m.kickoff_time):: varchar || '-' || (year(m.kickoff_time) + 1):: varchar when 
            month(m.kickoff_time) in (1,2,3,4,5) then 
            (year(m.kickoff_time) - 1):: varchar || '-' ||  year(m.kickoff_time):: varchar end as Season from src_matches m)) as Season
            from  el 
            join  t on el.team = t.id
            join  elt on el.element_type = elt.id 
            where elt.pos_name != 'Goalkeeper' order by event_points desc, now_cost asc 
            limit 10)