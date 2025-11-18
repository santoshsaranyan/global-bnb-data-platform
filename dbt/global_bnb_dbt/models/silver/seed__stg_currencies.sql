WITH stg_currencies AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'country',
            'currency_code'
        ]) }} AS currency_id,
        country,
        currency_code,
        currency_name
    FROM {{ ref('seed__country_currencies') }}
)

SELECT * FROM stg_currencies