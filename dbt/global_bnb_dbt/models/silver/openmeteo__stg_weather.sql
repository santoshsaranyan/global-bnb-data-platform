WITH ranked_weather AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city, country, date
            ORDER BY extract_month DESC
        ) AS rn
    FROM {{ source('bronze', 'openmeteo__raw_weather') }}
),

WITH stg_weather AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'date',
            'city',
            'country'
        ]) }} AS weather_id,
        date::DATE AS weather_date,
        temperature_2m_max::NUMERIC(5,2),
        temperature_2m_min::NUMERIC(5,2),
        temperature_2m_mean::NUMERIC(5,2),
        precipitation_sum::NUMERIC(7,3),
        city,
        country,
        extract_month
    FROM ranked_weather
    WHERE rn = 1
)

SELECT * FROM stg_weather