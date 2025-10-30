with e as (
    select * from {{ ref("dim_events") }}   QUALIFY row_number() over (partition by sid order by sid) = 1
),
el as (
    select distinct(id) as id, web_name, second_name  from {{ ref("src_elements")}} 
),
el1 as (
    select distinct(id) as id, web_name, second_name  from {{ ref("src_elements")}} 
),
el2 as (
    select distinct(id) as id, web_name, second_name  from {{ ref("src_elements")}} 
),
el3 as (
    select distinct(id) as id, web_name, second_name  from {{ ref("src_elements")}} 
),
m as (
    select * from {{ ref("src_matches")}} 
)



select e.id, name, average_entry_score as Average_Points, highest_score as Highest_Points,
               top_element_info__points as Points, transfers_made as Gameweek_Transfers,
                el.web_name as Most_Transferred_In, el1.web_name as
                Most_Selected,el2.web_name as Most_Captained, el3.web_name as Most_Points,
                (select max(Season) as Season  from (select e.deadline_time, case when month(e.deadline_time) in (8, 
                9,10,12,11) then year(e.deadline_time):: varchar || '-' || (year(e.deadline_time) + 1):: varchar when 
                month(e.deadline_time) in (1,2,3,4,5) then 
                (year(e.deadline_time) - 1):: varchar || '-' ||  year(e.deadline_time):: varchar end as Season from  m)) as Season
            from 
               e join  el on e.most_transferred_in = el.id join  el1 on e.most_selected
                = el1.id join  el2 on e.most_captained = el2.id join  el3 on
                 e.top_element = el3.id
                where finished=True order by name