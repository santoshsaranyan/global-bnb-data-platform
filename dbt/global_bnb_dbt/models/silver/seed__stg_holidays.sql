WITH stg_holidays AS (
    SELECT
        *
    FROM {{ ref('seed__public_holidays') }}
)

select * from stg_holidays