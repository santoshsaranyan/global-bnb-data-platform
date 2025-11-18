WITH stg_holidays AS (
    SELECT
        "countryOrRegion" AS country,
        date::DATE AS holiday_date,
        "holidayName" AS holiday_name,
        "normalizeHolidayName" AS normalized_holiday_name,
        "isPaidTimeOff" AS is_paid_time_off,
        "countryRegionCode" AS country_code
    FROM {{ ref('seed__public_holidays') }}
),
final_stg_holidays AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'country',
            'holiday_date',
            'holiday_name'
        ]) }} AS holiday_id,
        country,
        holiday_date,
        holiday_name,
        normalized_holiday_name,
        is_paid_time_off,
        country_code
    FROM stg_holidays
)

SELECT * FROM final_stg_holidays