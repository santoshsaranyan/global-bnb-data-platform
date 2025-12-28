WITH base_trends AS (
    SELECT
        INITCAP(city) AS city,
        INITCAP(country) AS country,
        trend_date,
        visit_city AS visit_city_index,
        things_to_do_in_city AS things_to_do_index,
        city_airbnb AS city_airbnb_index
    FROM {{ source('silver', 'googletrends__stg_trends') }}
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

fact_market_trends AS (
    SELECT
        dd.date_id,
        dl.location_id,
        bt.visit_city_index,
        bt.things_to_do_index,
        bt.city_airbnb_index
    FROM base_trends AS bt
    LEFT JOIN dim_locations AS dl
        ON bt.city = dl.city
        AND bt.country = dl.country
    LEFT JOIN dim_dates AS dd
        ON bt.trend_date = dd.date
)