WITH stg_currencies AS (
    SELECT
        *
    FROM {{ ref('seed__country_currencies') }}
)

select * from stg_currencies