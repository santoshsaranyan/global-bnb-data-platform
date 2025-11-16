WITH stg_trends AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'city',
            'country',
            'extract_month'
        ]) }} AS trend_id,
        visit_city::INTEGER,
        things_to_do_in_city::INTEGER,
        city_airbnb::INTEGER,
        CASE
            WHEN "isPartial" = 'True' THEN TRUE
            WHEN "isPartial" = 'False' THEN FALSE
        END AS is_partial,
        city,
        country,
        extract_month
    FROM {{ source('bronze', 'googletrends__raw_trends') }}
)

select * from stg_trends