WITH base_listings AS (
    SELECT
        listing_id,
        host_cid,
        INITCAP(city) AS city,
        INITCAP(country) AS country,
        price AS nightly_price,
        minimum_nights,
        maximum_nights
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

dim_dates AS (
    SELECT 
        date_id,
        date
    FROM {{ source('gold', 'dim_dates') }}
),

base_calendar AS (
    SELECT
        listing_id,
        calendar_date,
        is_available
    FROM {{ source('silver', 'insideairbnb__stg_calendar') }}
),

fact_listings AS (
    SELECT
        bl.listing_id,
        dh.host_id,
        dl.location_id,
        dd.date_id,
        bl.nightly_price,
        bl.minimum_nights,
        bl.maximum_nights,
        bc.is_available,
        CASE 
            WHEN bc.is_available = TRUE THEN 1
            ELSE 0
        END AS estimated_occupancy,
        estimated_occupancy * bl.nightly_price AS estimated_revenue
    FROM base_listings AS bl
    LEFT JOIN dim_hosts AS dh
        ON bl.host_cid = dh.host_cid
    LEFT JOIN dim_locations AS dl
        ON bl.city = dl.city
        AND bl.country = dl.country
    LEFT JOIN base_calendar AS bc
        ON bl.listing_id = bc.listing_id
    LEFT JOIN dim_dates AS dd
        ON bc.calendar_date = dd.date
)