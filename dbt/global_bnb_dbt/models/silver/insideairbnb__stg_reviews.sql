WITH ranked_reviews AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city, country, id, listing_id, date
            ORDER BY extract_month DESC
        ) AS rn
    FROM {{ source('bronze', 'insideairbnb__raw_reviews') }}
),
WITH stg_reviews AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'listing_id',
            'id',
            'date',
            'city',
            'country'
        ]) }} AS review_id,
        listing_id AS listing_cid,
        id AS review_cid,
        date::DATE AS review_date,
        reviewer_id,
        COALESCE(reviewer_name,'Unknown') AS reviewer_name,
        COALESCE(comments,'No comments provided') AS comments,
        city,
        country,
        extract_month
    FROM ranked_reviews
    WHERE rn = 1
)

SELECT * FROM stg_reviews