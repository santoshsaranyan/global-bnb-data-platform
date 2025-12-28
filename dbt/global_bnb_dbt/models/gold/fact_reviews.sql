WITH base_reviews AS (
    SELECT 
        listing_id,
        host_cid,
        INITCAP(city) AS city,
        INITCAP(country) AS country,
        overall_rating,
        cleanliness_rating,
        communication_rating,
        checkin_rating,
        accuracy_rating,
        value_rating,
        location_rating,
        number_of_reviews AS review_count
    FROM {{ source('silver', 'insideairbnb__stg_listings') }}
),

dim_hosts AS (
    SELECT 
        host_id,
        host_cid
    FROM {{ source('gold', 'dim_hosts') }}
),

dim_locations AS (
    SELECT 
        location_id,
        city,
        country
    FROM {{ source('gold', 'dim_locations') }}
),

fact_reviews AS (
    SELECT
        br.listing_id,
        dh.host_id,
        dl.location_id,
        br.overall_rating,
        br.cleanliness_rating,
        br.communication_rating,
        br.checkin_rating,
        br.accuracy_rating,
        br.value_rating,
        br.location_rating,
        br.review_count
    FROM base_reviews AS br
    LEFT JOIN dim_hosts AS dh
        ON br.host_cid = dh.host_cid
    LEFT JOIN dim_locations AS dl
        ON br.city = dl.city
        AND br.country = dl.country
)

SELECT * FROM fact_reviews