{% test not_future_date(model, column_name) %}
    WITH  validation_errors AS(
        SELECT
            {{ column_name }}
        FROM {{ model }}
        WHERE {{ column_name }} > CURRENT_DATE()
    )
    select *
    from validation_errors
{% endtest %}