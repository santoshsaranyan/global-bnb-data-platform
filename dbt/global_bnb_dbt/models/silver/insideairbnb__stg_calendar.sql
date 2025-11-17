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
        price::NUMERIC(10,2),
        adjusted_price::NUMERIC(10,2),
        minimum_nights::INTEGER,
        maximum_nights::INTEGER,
        city,
        country,
        extract_month
    FROM {{ source('bronze', 'insideairbnb__raw_calendar') }}
)

select * from stg_calendar