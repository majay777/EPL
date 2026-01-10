with raw_elements as (
    select * from {{ source('epl_duckdb',  'elements')
        }} ),
raw_events  as (
    select * from {{ source('epl_duckdb', 'events') }}
    )


select erte.*, erte2.id as 'event' from raw_elements erte left join
raw_events erte2 on erte."_dlt_parent_id" = erte2."_dlt_parent_id"
where erte2.is_current =True