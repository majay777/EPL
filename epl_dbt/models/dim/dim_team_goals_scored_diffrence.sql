with team_goals as (
    select *
    from {{ ref("dim_team_goals") }}
    where true
    {% if is_incremental() %}
    and created_at > (select max(created_at) from {{ this }})
    {% endif %}
),
goals_scored as (
    select team_id, club, sum(goals_scored) as Goals_Scored, Season
    from team_goals
    group by team_id, club, Season
),
goals_conceded as (
    select team_id, club, sum(goals_conceded) as Goals_conceded, Season
    from team_goals
    group by team_id, club, Season
)

select gs.team_id,
       gs.club, 
       gs.Goals_Scored, 
       gc.Goals_conceded, 
       gs.Goals_Scored - gc.Goals_conceded as Goal_difference, 
       gs.Season
from goals_scored gs
join goals_conceded gc on gs.team_id = gc.team_id and gs.Season = gc.Season
