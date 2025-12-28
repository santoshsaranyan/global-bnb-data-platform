WITH dim_listings AS (
    SELECT 
        listing_id,
        listing_cid,
        listing_name,
        property_type,
        room_type,
        bathrooms,
        bathroom_type,
        bedrooms,
        beds,
        accommodates,
        instant_bookable,
        latitude,
        longitude,
        listing_neighbourhood_cleansed AS listing_neighbourhood
    FROM {{ source('silver', 'insideairbnb__stg_listings') }}
)

SELECT * FROM dim_listings

