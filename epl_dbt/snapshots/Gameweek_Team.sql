{% snapshot scd_team_of_gameweek %}

{{
    config(
        target_schema='dev',
        unique_key= 'gameweek',
        strategy='check',
        check_cols=['gameweek', 'Season'],
        invalidate_hard_deletes=True
    )
}}

select * from  {{ ref("dim_team_of_gameweek") }}

{% endsnapshot %}

git remote add origin git@github.com:<your-username>/<repo-name>.git
