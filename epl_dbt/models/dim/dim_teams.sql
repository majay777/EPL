select distinct
    sid,
    name,
    short_name,
    code,
    id,
    strength,
    strength_overall_home,
    strength_overall_away,
    strength_attack_home,
    strength_attack_away,
    strength_defence_home,
    strength_defence_away,
    pulse_id,
    Season
from {{ ref("src_teams") }}