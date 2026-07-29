{% macro get_latest_load_date(parent_table) %}
    {% set result = run_query("SELECT load_date FROM " ~ parent_table ~ " ORDER BY load_date DESC LIMIT 1") %}
    {% if execute %}
        {{ return(result.columns[0].values()[0]) }}
    {% endif %}
{% endmacro %}
