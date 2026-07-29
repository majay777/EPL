{% macro calculate_season(date_column) %}
    case 
        when month({{ date_column }}) in (6, 7, 8, 9, 10, 11, 12) 
        then year({{ date_column }})::varchar || '-' || (year({{ date_column }}) + 1)::varchar
        when month({{ date_column }}) in (1, 2, 3, 4, 5) 
        then (year({{ date_column }}) - 1)::varchar || '-' || year({{ date_column }})::varchar
    end
{% endmacro %}

