

with raw_element_types as (
select * from {{ source('epl_duckdb', 'element_types') }} QUALIFY row_number() over (partition by id order by plural_name) = 1
)
select
      distinct(id),
      plural_name,
      plural_name_short,
      singular_name as pos_name,
      singular_name_short as pos_name_short,
      squad_select as per_team_sel,
      squad_min_play as per_team_min,
      squad_max_play as per_team_max,
      ui_shirt_specific,
      element_count as no_of_players, current_date as created_at,
      {{ dbt_utils.generate_surrogate_key(['id', 'plural_name', 'singular_name']) }} as sid
      from raw_element_types
