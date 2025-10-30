select
   *
   from {{ ref("src_chip_plays") }}
   where num_played < 0