with gms as (
    select * from {{ ref("dim_goals_scored_home") }}
    where true
     {% if is_incremental() %}
        and created_at > (select max(created_at) from {{ this }})
    {% endif %}
),
gds as (
    select * from {{ ref("dim_goals_scored_away") }}
    where true
     {% if is_incremental() %}
        and created_at > (select max(created_at) from {{ this }})
    {% endif %}
),
gmc as (
    select * from {{ ref("dim_goals_conceded_home") }}
    where true
     {% if is_incremental() %}
        and created_at > (select max(created_at) from {{ this }})
    {% endif %}
),
gdc as (
    select * from {{ ref("dim_goals_conceded_away") }}
    where true
     {% if is_incremental() %}
        and created_at > (select max(created_at) from {{ this }})
    {% endif %}
),
t as  
(
    select CLUB, Goals_scored_home + goals_scored_away as Goals_Scored, Season from (select gms.club, 
                gms.goals_scored as Goals_Scored_home, gds.goals_scored as Goals_scored_away, gms.Season from
                 gms join
                 gds on gms.club = gds.club) 
),
t1 as (
                select CLUB, Goals_conceded_home + 
                Goals_conceded_away as Goals_conceded from (select gmc.club,
                gmc.goals_conceded as Goals_conceded_home, gdc.goals_conceded as Goals_conceded_away from
                 gmc join
                 gdc on gmc.club = gdc.club)
)

select t.club, Goals_Scored, Goals_conceded, Goals_scored - Goals_conceded as Goal_difference, t.Season from t join t1 on t.club=t1.club
