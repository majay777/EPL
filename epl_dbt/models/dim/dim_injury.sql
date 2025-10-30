with e as (select * from {{ ref("src_elements")}}

),
et as (
    select * from {{ ref("src_element_types") }}

),
t as (
    select * from {{ ref("dim_teams") }}

)

select  distinct(e.web_name) as NAME ,et.pos_name_short as POS , t.short_name as CLUB, E.NEWS as NEWS,
    e.news_added as NEWS_DATED, case when month(e.news_added) in (7,8, 
            9,10,12,11) then year(e.news_added):: varchar || '-' || (year(e.news_added) + 1):: varchar when 
            month(e.news_added) in (1,2,3,4,5) then 
            (year(e.news_added) - 1):: varchar || '-' ||  year(e.news_added):: varchar
            end as Season
    from  e join
    et on e.element_type = et.id
    join  t
    on e.team = t.id   and e.news not like '%Transfer%' and e.news != ''
    order by e.news_added desc