with e as (select *
           from {{ ref("src_elements")}}),
     et as (select *
            from {{ ref("src_element_types") }}),
     t as (select *
           from {{ ref("dim_teams") }})

select distinct(e.web_name)      as NAME,
               et.pos_name_short as POS,
               t.name            as CLUB,
               E.NEWS            as NEWS,
               e.news_added      as NEWS_DATED,
               e.Season
    from  e join
    et on e.element_type = et.id
    join  t
    on e.team = t.id and e.Season = t.Season and e.news not like '%Transfer%' and e.news != ''
    order by e.news_added desc