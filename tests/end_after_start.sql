{% test end_after_start(model, start_column, end_column) %}
    WITH validation_errors AS (
        SELECT
            *
        FROM {{ model }}
        WHERE {{ end_column }} <= {{ start_column }}
    )
    SELECT *
    FROM validation_errors
{% endtest %}
