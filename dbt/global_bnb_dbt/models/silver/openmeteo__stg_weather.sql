WITH stg_weather AS (
    SELECT
        *
    FROM {{ source('bronze', 'openmeteo__raw_weather') }}
)

select * from stg_weather;