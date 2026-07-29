with el as (select *
            from (select *, row_number() over (partition by id, Season, web_name order by id, "_dlt_list_idx" desc) as rnk
                  from {{ ref("src_elements") }})
            where rnk = 1),
     t as (select *
           from {{ ref("dim_teams") }}),
     elt as (select *
             from {{ ref("src_element_types") }}),
     m as (select *
           from {{ ref("src_matches")}})

    (select distinct(elt.pos_name) as                             POS,
                    el.web_name    as                             PLAYER_NAME,
                    t.name         as                             club,
                    el.event_points as                            Points,
                    (Select mx.event from m mx where mx.finished = 'true' and mx.Season = el.Season
                     order by mx.code desc limit 1) as            gameweek,
                    el.Season                                     as Season
from el
join t on el.team = t.id and el.Season = t.Season
join elt on el.element_type = elt.id
where elt.pos_name = 'Goalkeeper'
order by Points desc, el.now_cost asc limit 1)
union all
(
select distinct (elt.pos_name) as                                 POS, 
                el.web_name as                                    PLAYER_NAME, 
                t.name as                                         club, 
                el.event_points as                                Points, 
                (Select mx.event from m mx where mx.finished = 'true' and mx.Season = el.Season 
                 order by mx.code desc limit 1) as                gameweek, 
                el.Season                                     as  Season
from el
join t on el.team = t.id and el.Season = t.Season
join elt on el.element_type = elt.id
where elt.pos_name != 'Goalkeeper'
order by Points desc, el.now_cost asc
limit 10)