{{ config(
    materialized = 'view'
) }}

WITH fact_listings AS (
    SELECT
        listing_id,
        location_id,
        date_id,
        nightly_price,
        estimated_occupancy,
        estimated_revenue
    FROM {{ ref('fact_listings') }}
),

dim_dates AS (
    SELECT
        date_id,
        date,
        year,
        month,
        month_name,
        week_of_year,
        day_name,
        is_weekend,
        is_holiday,
        holiday_name
    FROM {{ ref('dim_dates') }}
),

dim_locations AS (
    SELECT
        location_id,
        city,
        country,
        region,
        currency_code
    FROM {{ ref('dim_locations') }}
),

fact_weather AS (
    SELECT
        date_id,
        location_id,
        temperature_2m_mean,
        precipitation_sum
    FROM {{ ref('fact_weather') }}
),

fact_market_trends AS (
    SELECT
        date_id,
        location_id,
        visit_city_index,
        things_to_do_index,
        city_airbnb_index
    FROM {{ ref('fact_market_trends') }}
),

market_performance AS (
    SELECT
        dd.date,
        dd.year,
        dd.month,
        dd.month_name,
        dd.week_of_year,
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        dd.holiday_name,

        dl.country,
        dl.region,
        dl.city,
        dl.currency_code,

        COUNT(DISTINCT fl.listing_id) AS active_listings,
        AVG(fl.nightly_price) AS avg_nightly_price,
        SUM(fl.estimated_occupancy) AS estimated_booked_nights,
        SUM(fl.estimated_revenue) AS estimated_revenue,

        fw.temperature_2m_mean,
        fw.precipitation_sum,

        fmt.visit_city_index,
        fmt.things_to_do_index,
        fmt.city_airbnb_index

    FROM fact_listings AS fl
    LEFT JOIN dim_dates AS dd
        ON fl.date_id = dd.date_id
    LEFT JOIN dim_locations AS dl
        ON fl.location_id = dl.location_id
    LEFT JOIN fact_weather AS fw
        ON fl.date_id = fw.date_id
       AND fl.location_id = fw.location_id
    LEFT JOIN fact_market_trends AS fmt
        ON fl.date_id = fmt.date_id
       AND fl.location_id = fmt.location_id

    GROUP BY
        dd.date,
        dd.year,
        dd.month,
        dd.month_name,
        dd.week_of_year,
        dd.day_name,
        dd.is_weekend,
        dd.is_holiday,
        dd.holiday_name,
        dl.country,
        dl.region,
        dl.city,
        dl.currency_code,
        fw.temperature_2m_mean,
        fw.precipitation_sum,
        fmt.visit_city_index,
        fmt.things_to_do_index,
        fmt.city_airbnb_index
)

SELECT * FROM market_performance
