{% macro add_load_date_to_child(child_table, parent_table) %}
    {% set latest_load_date = get_latest_load_date(parent_table) %}

    SELECT
        *,
        '{{ latest_load_date }}' AS load_date
    FROM {{ child_table }}
{% endmacro %}
