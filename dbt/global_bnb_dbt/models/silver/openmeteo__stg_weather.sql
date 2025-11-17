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
    FROM {{ source('bronze', 'openmeteo__raw_weather') }}
)

select * from stg_weather