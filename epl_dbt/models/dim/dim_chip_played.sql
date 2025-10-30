with c as (
    select * from {{ ref("src_chip_plays")}}  QUALIFY row_number() over (partition by sid  order by sid) = 1
),
e as (
    select * from {{ ref("dim_events") }}   QUALIFY row_number() over (partition by sid order by sid) = 1
)

select  chip_name, num_played, e.id, e.name,
               (select max(Season) as Season  from (select e.deadline_time, case when month(e.deadline_time) in (8, 
                9,10,12,11) then year(e.deadline_time):: varchar || '-' || (year(e.deadline_time) + 1):: varchar when 
                month(e.deadline_time) in (1,2,3,4,5) then 
                (year(e.deadline_time) - 1):: varchar || '-' ||  year(e.deadline_time):: varchar end as Season from e)) as Season
                from   c
                right join  e on c._dlt_parent_id = e._dlt_id where e.finished = 'True'  order by id