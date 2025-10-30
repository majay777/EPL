with se as (
select * from {{ ref("src_events") }}
)

select distinct(sid) as sid , id, Season, name, deadline_time, average_entry_score, finished, data_checked,
                      highest_scoring_entry,
                       deadline_time_epoch, deadline_time_game_offset, highest_score, is_previous, is_current, is_next,
                       cup_leagues_created, h2h_ko_matches_created, ranked_count, most_selected, most_transferred_in,
                      top_element, top_element_info__id, top_element_info__points, transfers_made, most_captained,
                      most_vice_captained, _dlt_parent_id, _dlt_list_idx, _dlt_id from se