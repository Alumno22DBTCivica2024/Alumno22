{% test date_format(model, column_name) %}
    WITH validation_errors AS (
        SELECT
            {{ column_name }}
        FROM {{ model }}
        WHERE NOT REGEXP_LIKE({{ column_name }}, '^\d{4}/(0[1-9]|1[0-2])$')
    )
    SELECT *
    FROM validation_errors
{% endtest %}

