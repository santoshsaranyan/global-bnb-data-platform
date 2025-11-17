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
        COALESCE(reviewer_name,'Unknown') as reviewer_name,
        COALESCE(comments,'No comments provided') as comments,
        city,
        country,
        extract_month
    FROM {{ source('bronze', 'insideairbnb__raw_reviews') }}
)

select * from stg_reviews