WITH base_weather AS (
    SELECT
        INITCAP(city) AS city,
        INITCAP(country) AS country,
        temperature_2m_max,
        temperature_2m_min,
        temperature_2m_mean,
        precipitation_sum,
        weather_date
    FROM {{ source('silver', 'openmeteo__stg_weather') }}
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

fact_weather AS (
    SELECT
        dd.date_id,
        dl.location_id,
        bw.temperature_2m_max,
        bw.temperature_2m_min,
        bw.temperature_2m_mean,
        bw.precipitation_sum
    FROM base_weather AS bw
    LEFT JOIN dim_locations AS dl
        ON bw.city = dl.city
        AND bw.country = dl.country
    LEFT JOIN dim_dates AS dd
        ON bw.weather_date = dd.date
)