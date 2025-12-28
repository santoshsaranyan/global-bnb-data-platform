WITH listing_descriptions AS (
    SELECT 
        listing_id,
        listing_cid,
        host_cid,
        listing_name,
        listing_description,
        neighborhood_overview,
        amenities,
        INITCAP(city) AS city,
        INITCAP(country) AS country,
    FROM {{ source('silver', 'insideairbnb__stg_listings') }}
)

SELECT * FROM listing_descriptions

