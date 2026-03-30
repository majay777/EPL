with matches as (select *
                 from {{ ref('src_matches') }}),

     elements as (select *
                  from (select *,
                               row_number() over (
                   partition by id, event
                   order by "_dlt_list_idx" desc
               ) as rn
                        from {{ ref('src_elements_event') }})
                  where rn = 1),

     events as (select *
                from (select *,
                             row_number()
                                 over(partition by id order by "_dlt_list_idx" desc )
                                as rn
                      from {{ ref('dim_events') }})
                where rn = 1
                order by id),

     teams as (select *
               from {{ ref('dim_teams') }}),

     players as (select *
                 from (select *,
                              row_number() over (partition by player_natural_key order by minutes desc)
                                as rn
                       from {{ ref('dim_players_scd2') }})
                 where rn = 1
                 order by player_natural_key),


     final as (select
    {{ dbt_utils.generate_surrogate_key([
    'el.id', 'm.id', 'ev.id'
    ]) }} as fact_sk, el.id as player_natural_key, p.player_sk, ev.id as event_id, ev.name as event_name, m.id as match_id, m.kickoff_time as matched_at, t_opp.name as opponent, t_club.name as club, case
    when m.team_h_score = m.team_a_score then 'D'
    when el.team = m.team_h and m.team_h_score > m.team_a_score then 'W'
    when el.team = m.team_a and m.team_a_score > m.team_h_score then 'W'
    else 'L'
end
as result,

        el.event_points                  as gameweek_points,
        el.total_points,
        el.minutes,
        el.starts,
        el.goals_scored,
        el.assists,
        el.expected_goals,
        el.expected_assists,
        el.clean_sheets,
        el.goals_conceded,
        el.bonus,
        el.bps,
        el.influence,
        el.creativity,
        el.threat,
        el.ict_index,
        el.now_cost                      as cost,

        current_timestamp                as updated_at

    from matches m
    join elements el
        on m.event = el.event
    join events ev
        on m.event = ev.id
    join teams t_club
        on el.team = t_club.id
    join teams t_opp
        on (
            (el.team = m.team_h and t_opp.id = m.team_a)
            or
            (el.team = m.team_a and t_opp.id = m.team_h)
        )

    -- ✅ SCD2-aware player snapshot join
    join players p
        on el.id = p.player_natural_key
--        and m.kickoff_time >= p.valid_from
--        and m.kickoff_time < coalesce(p.valid_to, timestamp '9999-12-31')
)


select *
from final
