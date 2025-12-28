WITH base_locations AS (
    SELECT DISTINCT
        INITCAP(country) AS country,
        INITCAP(city) AS city
    FROM {{ source('silver', 'insideairbnb__stg_listings') }}
),

base_currency AS (
    SELECT
        INITCAP(country) AS country,
        currency_code,
        INITCAP(currency_name) AS currency_name
    FROM {{ source('silver', 'seed__stg_currencies') }}
),

base_country AS (
    SELECT
        INITCAP(country) AS country,
        country_code,
        region
    FROM {{ source('silver', 'seed__stg_countries') }}
),

dim_locations AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'city',
            'country'
        ]) }} AS location_id,
        bl.city,
        bl.country,
        bco.country_code,
        bco.region,
        bc.currency_code,
        bc.currency_name
    FROM base_locations AS bl
    LEFT JOIN base_currency AS bc
        ON bl.country = bc.country
    LEFT JOIN base_country AS bco
        ON bl.country = bco.country
)

SELECT * FROM dim_locations