WITH stg_calendar AS (
    SELECT
        *
    FROM {{ source('bronze', 'insideairbnb__raw_calendar') }}
)

select * from stg_calendar