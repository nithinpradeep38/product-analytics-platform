{% macro is_weekend(day_of_week_column) %}
    case when {{ day_of_week_column }} in (0, 6) then true else false end
{% endmacro %}