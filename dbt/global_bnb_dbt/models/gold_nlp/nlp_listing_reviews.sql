WITH listing_reviews AS (
    SELECT 
        review_id,
        listing_cid,
        review_cid,
        review_date,
        comments AS review_comments,
        INITCAP(city) AS city,
        INITCAP(country) AS country,
    FROM {{ source('silver', 'insideairbnb__stg_reviews') }}
)

SELECT * FROM listing_reviews