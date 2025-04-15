{% test not_empty(model, column_name) %}

WITH 
validation_errors as (
    SELECT {{column_name}}
    FROM {{ model }}
    WHERE LEN({{column_name}}) = 0
)

SELECT * FROM validation_errors

{%- endtest %}