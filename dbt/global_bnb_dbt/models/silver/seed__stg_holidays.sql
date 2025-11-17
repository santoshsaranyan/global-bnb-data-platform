WITH stg_holidays AS (
    SELECT
        "countryOrRegion" as country,
        date::DATE as holiday_date,
        "holidayName" as holiday_name,
        "normalizeHolidayName" as normalized_holiday_name,
        "isPaidTimeOff" as is_paid_time_off,
        "countryRegionCode" as country_code
    FROM {{ ref('seed__public_holidays') }}
)

select * from stg_holidays