with raw_events as (
select * from {{ source('epl_duckdb', 'events') }}
)

--select *,{{ dbt_utils.generate_surrogate_key(['id', 'name']) }} as sid from raw_events


select id, name, deadline_time, average_entry_score, finished, data_checked, highest_scoring_entry,
deadline_time_epoch, deadline_time_game_offset, highest_score,
is_previous, is_current, is_next, cup_leagues_created, h2h_ko_matches_created, ranked_count,
most_selected, most_transferred_in, top_element, top_element_info__id,
top_element_info__points, transfers_made, most_captained, most_vice_captained, "_dlt_parent_id",
"_dlt_list_idx", "_dlt_id",case when month(deadline_time) in (8,
                       9,10,12,11) then year(deadline_time):: varchar || '-' || (year(deadline_time) + 1):: varchar when
                       month(deadline_time) in (1,2,3,4,5) then
                       (year(deadline_time) - 1):: varchar || '-' ||  year(deadline_time):: varchar
                       end as Season , {{ dbt_utils.generate_surrogate_key(['id', 'name', 'deadline_time']) }} as sid from
(select *, row_number() over (partition by _dlt_list_idx order by _dlt_parent_id) as rk
from raw_events) e where e.rk =1 order by id