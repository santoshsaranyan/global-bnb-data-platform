# Silver Data Catalog — Staging Models

This catalog documents all staging models in the InsideAirbnb, Google Trends, Open-Meteo, and seed layers of your warehouse.  
Each table includes column-level descriptions and associated tests.

---

## `insideairbnb__stg_listings`

**Staging model for InsideAirbnb listings.**

### Columns

| Column | Description | Tests |
|-------|-------------|-------|
| **listing_id** | Surrogate key for the listing (city + country + listing_cid). | not_null, unique |
| **listing_cid** | Source listing ID from InsideAirbnb. | not_null |
| **city** | City where the listing is located. | not_null |
| **country** | Country where the listing is located. | not_null |
| **extract_month** | YYYYMM batch month for the extract. | not_null |
| **source_last_scraped** | Date when the listing was last scraped by InsideAirbnb. | — |
| **source_platform** | Platform/source of the listing data. | — |
| **listing_name** | Listing title/name. | — |
| **listing_description** | Listing description. | — |
| **neighbourhood_overview** | Host's description of the neighbourhood. | — |
| **latitude** | Latitude of the listing. | — |
| **longitude** | Longitude of the listing. | — |
| **property_type** | Property type (e.g. Apartment, House, etc.). | — |
| **room_type** | Room type (Entire home/apt, Private room, etc.). | — |
| **accommodates** | Number of guests the listing accommodates. | — |
| **bathrooms** | Number of bathrooms. | — |
| **bathroom_type** | Derived from text (Shared / Private). | — |
| **beds** | Number of beds. | — |
| **bedrooms** | Number of bedrooms. | — |
| **price** | Nightly price, numeric value without currency symbols. | — |
| **has_availability** | Whether listing has availability. | accepted_values: true/false |
| **instant_bookable** | Whether listing is instantly bookable. | accepted_values: true/false |
| **host_cid** | Host identifier. | — |
| **host_name** | Host name. | — |
| **host_response_time** | Host response time. | — |
| **host_response_rate** | Host response rate (%). | — |
| **host_acceptance_rate** | Host acceptance rate (%). | — |
| **host_is_superhost** | Whether host is a Superhost. | accepted_values: true/false |
| **host_has_profile_pic** | Whether host has a profile picture. | accepted_values: true/false |
| **host_identity_verified** | Whether host identity is verified. | accepted_values: true/false |
| **overall_rating** | Overall review score rating. | — |
| **accuracy_rating**, **cleanliness_rating**, **checkin_rating**, **communication_rating**, **location_rating**, **value_rating** | Individual rating components. | — |
| **license** | License information (defaulted if missing). | — |
| **number_of_reviews** | Total number of reviews. | — |
| **number_of_reviews_ltm** | Reviews in last 12 months. | — |
| **number_of_reviews_l30d** | Reviews in last 30 days. | — |
| **estimated_occupancy_l365d** | Estimated occupancy in last year. | — |
| **estimated_revenue_l365d** | Estimated revenue in last year. | — |
| **first_review** | Date of first review. | — |
| **last_review** | Date of most recent review. | — |
| **reviews_per_month** | Avg. reviews per month. | — |
| **listing_neighbourhood** | Neighbourhood of listing. | — |
| **listing_neighbourhood_cleansed** | Normalized neighbourhood. | — |
| **neighbourhood_group_cleansed** | Higher-level neighbourhood group. | — |
| **calculated_host_listings_count** | Total listings for host. | — |
| **calculated_host_listings_count_entire_homes** | Host’s entire-home listings. | — |
| **calculated_host_listings_count_private_rooms** | Host’s private-room listings. | — |
| **calculated_host_listings_count_shared_rooms** | Host’s shared-room listings. | — |

---

## `insideairbnb__stg_reviews`

**Staging model for InsideAirbnb listing reviews.**

### Columns

| Column | Description | Tests |
|-------|-------------|-------|
| **review_id** | Surrogate key (listing + review + date + geography). | not_null, unique |
| **listing_cid** | ID of listing the review belongs to. | not_null |
| **review_cid** | Source review ID. | not_null |
| **review_date** | Date of the review. | not_null |
| **reviewer_id** | Reviewer ID. | — |
| **reviewer_name** | Reviewer name. | — |
| **comments** | Review text. | — |
| **city** | City of the listing being reviewed. | not_null |
| **country** | Country. | not_null |
| **extract_month** | YYYYMM batch month. | not_null |

---

## `insideairbnb__stg_calendar`

**Staging model for daily calendar (availability + pricing).**

### Columns

| Column | Description | Tests |
|-------|-------------|-------|
| **calendar_id** | Surrogate key (listing + date + geography). | not_null, unique |
| **listing_cid** | Listing identifier. | not_null |
| **calendar_date** | Calendar date. | not_null |
| **is_available** | Availability flag. | not_null, accepted_values: true/false |
| **price** | Base price for the date. | — |
| **adjusted_price** | Adjusted price if provided. | — |
| **minimum_nights** | Minimum allowed stay. | — |
| **maximum_nights** | Maximum allowed stay. | — |
| **city** | City. | not_null |
| **country** | Country. | not_null |
| **extract_month** | YYYYMM batch month. | not_null |

---

## `googletrends__stg_trends`

**Staging model for Google Trends travel interest by city/country.**

### Columns

| Column | Description | Tests |
|-------|-------------|-------|
| **trend_id** | Surrogate key (date + geography). | not_null, unique |
| **trend_date** | Trend measurement date. | not_null |
| **visit_city** | Index for “visit <city>” query. | — |
| **things_to_do_in_city** | Index for “things to do in <city>” query. | — |
| **city_airbnb** | Index for “<city> airbnb” query. | — |
| **is_partial** | Whether trend is partial. | accepted_values: true/false |
| **city** | City. | not_null |
| **country** | Country. | not_null |
| **extract_month** | YYYYMM batch month. | not_null |

---

## `openmeteo__stg_weather`

**Staging model for Open-Meteo daily weather observations.**

### Columns

| Column | Description | Tests |
|-------|-------------|-------|
| **weather_id** | Surrogate key (date + geography). | not_null, unique |
| **weather_date** | Observation date. | not_null |
| **temperature_2m_max** | Daily max temperature (°C). | — |
| **temperature_2m_min** | Daily min temperature (°C). | — |
| **temperature_2m_mean** | Daily mean temperature (°C). | — |
| **precipitation_sum** | Total precipitation (mm). | — |
| **city** | City. | not_null |
| **country** | Country. | not_null |
| **extract_month** | YYYYMM batch month. | not_null |

---

## `seed__stg_holidays`

**Staging model for Azure Public Holidays seed.**

### Columns

| Column | Description | Tests |
|-------|-------------|-------|
| **holiday_id** | Surrogate key (country + date + name). | not_null, unique |
| **country** | Country/region. | not_null |
| **holiday_date** | Holiday date. | not_null |
| **holiday_name** | Holiday name. | — |
| **normalized_holiday_name** | Normalized holiday name. | — |
| **is_paid_time_off** | Whether holiday is PTO. | accepted_values: true/false/NULL |
| **country_code** | Country code. | — |

---

## `seed__stg_currencies`

**Staging model for country-to-currency mapping.**

### Columns

| Column | Description | Tests |
|-------|-------------|-------|
| **currency_id** | Surrogate key (country + currency_code). | not_null, unique |
| **country** | Country name. | not_null |
| **currency_code** | 3-letter currency code. | not_null |
| **currency_name** | Currency name. | — |
