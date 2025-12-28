WITH base_dates AS (
    SELECT DISTINCT
        calendar_date AS date,

        EXTRACT(ISODOW FROM calendar_date)::int AS day_of_week,

        TRIM(TO_CHAR(calendar_date, 'Day')) AS day_name,

        EXTRACT(WEEK FROM calendar_date)::int AS week_of_year,

        EXTRACT(MONTH FROM calendar_date)::int AS month,

        TRIM(TO_CHAR(calendar_date, 'Month')) AS month_name,

        EXTRACT(QUARTER FROM calendar_date)::int AS quarter,

        EXTRACT(YEAR FROM calendar_date)::int AS year,

        CASE
            WHEN EXTRACT(ISODOW FROM calendar_date) IN (6, 7) THEN true
            ELSE false
        END AS is_weekend

    FROM {{ source('silver', 'insideairbnb__stg_calendar') }}
),

base_holidays AS (
    SELECT DISTINCT
        holiday_date::DATE AS date,
        normalized_holiday_name AS holiday_name
    FROM {{ source('silver', 'seed__stg_holidays') }}
),

dim_dates AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'date'
        ]) }} AS date_id,
        bd.date,
        bd.day_of_week,
        bd.day_name,
        bd.week_of_year,
        bd.month,
        bd.month_name,
        bd.quarter,
        bd.year,
        bd.is_weekend,
        CASE
            WHEN bh.holiday_name IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_holiday,
        bh.holiday_name
    FROM base_dates bd
    LEFT JOIN base_holidays bh
        ON bd.date = bh.date
)   

SELECT * FROM dim_dates