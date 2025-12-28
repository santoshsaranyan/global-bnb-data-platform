WITH stg_countries AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'country'
        ]) }} AS country_id,
        country,
        country_code,
        region
    FROM {{ ref('seed__country_codes') }}
)

SELECT * FROM stg_countries