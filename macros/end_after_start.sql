{% test end_after_start(model,column_name, start_column) %}
    WITH validation_errors AS (
        SELECT
            *
        FROM {{ model }}
        WHERE {{ column_name }} <= {{ start_column }}
    )
    SELECT *
    FROM validation_errors
{% endtest %}
