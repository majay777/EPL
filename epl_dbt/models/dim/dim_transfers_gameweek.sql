with e as (select *
           from (select *, row_number() over (partition by id, Season, web_name order by id, "_dlt_list_idx" desc) as rnk
                 from {{ ref("src_elements") }})
           where rnk = 1),
     et as (select *
            from {{ ref("src_element_types") }}),
     t as (select *
           from {{ ref("dim_teams") }}),
     m as (select *
           from {{ ref("src_matches") }}),

transfer_data as (
    select distinct(e.web_name) as NAME,
           et.pos_name_short as POS,
           t.short_name as CLUB,
           e.transfers_in_event,
           e.transfers_out_event,
           (Select mx.event
            from m mx
            where mx.finished = 'true' and mx.Season = e.Season
            order by mx.code desc limit 1) as gameweek,
           e.Season as Season
    from e
    join et on e.element_type = et.id
    join t on e.team = t.id and e.Season = t.Season
),

transfers_in as (
    select POS, NAME, CLUB, transfers_in_event as transfer_count, 'IN' as transfer_type, gameweek, Season,
           rank() over (partition by POS, NAME order by transfers_in_event desc) as RNK
    from transfer_data
),

transfers_out as (
    select POS, NAME, CLUB, transfers_out_event as transfer_count, 'OUT' as transfer_type, gameweek, Season,
           rank() over (partition by POS, NAME order by transfers_out_event desc) as RNK
    from transfer_data
)

select POS, NAME, CLUB, transfer_type, transfer_count, gameweek, Season
from transfers_in
where rnk = 1
union all
select POS, NAME, CLUB, transfer_type, transfer_count, gameweek, Season
from transfers_out
where rnk = 1
