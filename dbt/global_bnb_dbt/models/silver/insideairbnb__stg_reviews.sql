WITH stg_reviews AS (
    SELECT
        *
    FROM {{ source('bronze', 'insideairbnb__raw_reviews') }}
)

select * from stg_reviews