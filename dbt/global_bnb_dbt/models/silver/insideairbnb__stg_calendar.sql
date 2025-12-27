WITH ranked_calendar AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city, country, listing_id, date
            ORDER BY extract_month DESC
        ) AS rn
    FROM {{ source('bronze', 'insideairbnb__raw_calendar') }}
),

WITH stg_calendar AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'listing_id',
            'date',
            'city',
            'country'
        ]) }} AS calendar_id,
        listing_id AS listing_cid,
        date::DATE AS calendar_date,
        CASE
            WHEN available = 't' THEN TRUE
            WHEN available = 'f' THEN FALSE
        END AS is_available,
        REGEXP_REPLACE(price,'[^0-9\.]', '', 'g')::NUMERIC(10,2) AS price,
        REGEXP_REPLACE(adjusted_price,'[^0-9\.]', '', 'g')::NUMERIC(10,2) AS adjusted_price,
        minimum_nights::INTEGER,
        maximum_nights::INTEGER,
        city,
        country,
        extract_month
    FROM ranked_calendar
    WHERE rn = 1
)

SELECT * FROM stg_calendar