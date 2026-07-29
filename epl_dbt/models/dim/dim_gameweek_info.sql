with e as (select *
           from {{ ref("dim_events") }}
    QUALIFY row_number() over (partition by sid order by sid) = 1
    )
    , el as (
select distinct id, Season, web_name, second_name
from {{ ref ("src_elements")}}
    ), m as (
select *
from {{ ref ("src_matches")}}
    )

select distinct(e.id)                   as id,
               name,
               average_entry_score      as Average_Points,
               highest_score            as Highest_Points,
               top_element_info__points as Points,
               transfers_made           as Gameweek_Transfers,
               el.web_name              as Most_Transferred_In,
               el1.web_name             as Most_Selected,
               el2.web_name             as Most_Captained,
               el3.web_name             as Most_Points,
               e.Season                 as Season
            from
               e join  el on e.most_transferred_in = el.id and e.Season = el.Season
                 join  el as el1 on e.most_selected = el1.id and e.Season = el1.Season
                 join  el as el2 on e.most_captained = el2.id and e.Season = el2.Season
                 join  el as el3 on e.top_element = el3.id and e.Season = el3.Season
                 where e.finished=True or e.is_current=True order by name