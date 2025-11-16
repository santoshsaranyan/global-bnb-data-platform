WITH stg_trends AS (
    SELECT
        *
    FROM {{ source('bronze', 'googletrends__raw_trends') }}
)

select * from stg_trends