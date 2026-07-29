with c as (select *
           from {{ ref("src_chip_plays")}}
    QUALIFY row_number() over (partition by sid order by sid) = 1
    )
   , e as (
select *
from {{ ref ("dim_events") }}
where finished= True
   or is_current= True QUALIFY row_number() over (partition by sid order by sid) = 1
    )

select c.chip_name,
       c.num_played,
       e.id,
       e.name,
       e.Season
from c
right join e on c._dlt_parent_id = e._dlt_id 
where e.finished = True or e.is_current = True 
order by e.id