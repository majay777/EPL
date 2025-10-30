with e as (
  select * from (select *, row_number() over (partition by id, web_name order by id, "_dlt_list_idx" desc) as rnk from
            {{ ref ("src_elements") }})  where rnk=1

),
et as (
    select * from {{ ref("src_element_types") }}

),
t as (
    select * from {{ ref("dim_teams") }}

),
m as (
    select * from {{ ref("src_matches") }}

)

select POS, NAME, CLUB, transfers_out_event as transfers_out_event, gameweek, Season
            from ( 
            select distinct(e.web_name) as NAME, et.pos_name_short as POS, t.short_name as CLUB, 
            e.transfers_out_event,(Select  event  from src_matches where finished = 'true' order by code desc limit 1) as gameweek,
            rank() over ( partition by POS, NAME order by e.transfers_out_event desc)  as RNK,
            (select max(Season) as Season  from (select m.kickoff_time, case when month(m.kickoff_time) in (8, 
            9,10,12,11) then year(m.kickoff_time):: varchar || '-' || (year(m.kickoff_time) + 1):: varchar when 
            month(m.kickoff_time) in (1,2,3,4,5) then 
            (year(m.kickoff_time) - 1):: varchar || '-' ||  year(m.kickoff_time):: varchar end as Season from src_matches m)) as Season from  
            e join 
            et on e.element_type = et.id join  t
            on e.team = t.id order by (e.transfers_out_event) desc 
            ) where rnk = 1
