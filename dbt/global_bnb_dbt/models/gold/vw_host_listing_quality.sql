{{ config(
    materialized = 'view'
) }}

WITH fact_listings AS (
    SELECT
        listing_id,
        host_id,
        location_id,
        nightly_price,
        estimated_occupancy,
        estimated_revenue
    FROM {{ ref('fact_listings') }}
),

fact_reviews AS (
    SELECT
        listing_id,
        host_id,
        location_id,
        review_count,
        overall_rating,
        cleanliness_rating,
        communication_rating,
        location_rating,
        value_rating
    FROM {{ ref('fact_reviews') }}
),

dim_hosts AS (
    SELECT
        host_id,
        host_name,
        host_is_superhost,
        host_response_rate,
        host_acceptance_rate,
        host_total_listings_count,
        host_identity_verified
    FROM {{ ref('dim_hosts') }}
),

dim_locations AS (
    SELECT
        location_id,
        city,
        country,
        region
    FROM {{ ref('dim_locations') }}
),

dim_listings AS (
    SELECT
        listing_id,
        listing_name,
        property_type,
        room_type,
        accommodates,
        bedrooms,
        bathrooms,
        instant_bookable
    FROM {{ ref('dim_listings') }}
),

host_listing_quality AS (
    SELECT
        dh.host_id,
        dh.host_name,
        dh.host_is_superhost,
        dh.host_response_rate,
        dh.host_acceptance_rate,
        dh.host_total_listings_count,
        dh.host_identity_verified,

        dl.city,
        dl.country,
        dl.region,

        dli.listing_id,
        dli.listing_name,
        dli.property_type,
        dli.room_type,
        dli.accommodates,
        dli.bedrooms,
        dli.bathrooms,
        dli.instant_bookable,

        AVG(fl.nightly_price) AS avg_nightly_price,
        SUM(fl.estimated_occupancy) AS estimated_booked_nights,
        SUM(fl.estimated_revenue) AS estimated_revenue,

        fr.review_count,
        fr.overall_rating,
        fr.cleanliness_rating,
        fr.communication_rating,
        fr.location_rating,
        fr.value_rating

    FROM fact_listings AS fl
    LEFT JOIN dim_hosts AS dh
        ON fl.host_id = dh.host_id
    LEFT JOIN dim_locations AS dl
        ON fl.location_id = dl.location_id
    LEFT JOIN dim_listings AS dli
        ON fl.listing_id = dli.listing_id
    LEFT JOIN fact_reviews AS fr
        ON fl.listing_id = fr.listing_id
       AND fl.host_id = fr.host_id
       AND fl.location_id = fr.location_id

    GROUP BY
        dh.host_id,
        dh.host_name,
        dh.host_is_superhost,
        dh.host_response_rate,
        dh.host_acceptance_rate,
        dh.host_total_listings_count,
        dh.host_identity_verified,
        dl.city,
        dl.country,
        dl.region,
        dli.listing_id,
        dli.listing_name,
        dli.property_type,
        dli.room_type,
        dli.accommodates,
        dli.bedrooms,
        dli.bathrooms,
        dli.instant_bookable,
        fr.review_count,
        fr.overall_rating,
        fr.cleanliness_rating,
        fr.communication_rating,
        fr.location_rating,
        fr.value_rating
)

SELECT * FROM host_listing_quality
