
{% macro has_default_record(model, column, value) %}
  select count(*) = 1 as has_default_record
  from {{ model }}
  where {{ column }} = '{{ value }}'
{% endmacro %}
