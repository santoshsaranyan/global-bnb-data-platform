WITH stg_holidays AS (
    SELECT
        *
    FROM {{ ref('public_holidays') }}
)

select * from stg_holidays;