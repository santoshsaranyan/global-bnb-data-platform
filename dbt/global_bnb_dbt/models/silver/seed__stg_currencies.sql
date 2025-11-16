WITH stg_currencies AS (
    SELECT
        *
    FROM {{ ref('country_currencies') }}
)

select * from stg_currencies;