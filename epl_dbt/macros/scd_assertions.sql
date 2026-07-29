{% macro scd2_assertions(model, business_key) %}
select *
from {{ model }}
group by {{ business_key }}
having sum(case when is_current then 1 else 0 end) != 1
    {% endmacro %}
