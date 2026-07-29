with raw_elements as (select *
                      from {{ source('epl_duckdb', 'elements')
                               }}),
     events as (select *
                from {{ ref('src_events') }}
                where is_current = True)


select erte.*, ev.id as event, ev.Season
from raw_elements erte
join
     events ev on erte."_dlt_parent_id" = ev."_dlt_parent_id"