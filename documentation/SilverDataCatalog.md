# Silver Data Catalog - Staging Models

This catalog documents all staging models in the silver layer of the data warehouse.

---

## `insideairbnb__stg_listings`

**Staging model for InsideAirbnb listings.**

### Columns

| Column | Description | Type |
|-------|-------------|------|
| **listing_id** | Surrogate key for the listing (city + country + listing_cid). | VARCHAR |
| **listing_cid** | Source listing ID from InsideAirbnb. | VARCHAR |
| **city** | City where the listing is located. | VARCHAR |
| **country** | Country where the listing is located. | VARCHAR |
| **extract_month** | YYYYMM batch month for the extract. | INTEGER |
| **source_last_scraped** | Date when the listing was last scraped by InsideAirbnb. | DATE |
| **source_platform** | Platform/source of the listing data. | VARCHAR |
| **listing_name** | Listing title/name. | VARCHAR |
| **listing_description** | Listing description. | VARCHAR |
| **neighbourhood_overview** | Host's description of the neighbourhood. | VARCHAR |
| **latitude** | Latitude of the listing. | NUMERIC(9,6) |
| **longitude** | Longitude of the listing. | NUMERIC(9,6) |
| **property_type** | Property type (e.g. Apartment, House, etc.). | VARCHAR |
| **room_type** | Room type (Entire home/apt, Private room, etc.). | VARCHAR |
| **accommodates** | Number of guests the listing accommodates. | INTEGER |
| **bathrooms** | Number of bathrooms. | NUMERIC(4,2) |
| **bathroom_type** | Derived from text (Shared / Private). | VARCHAR |
| **beds** | Number of beds. | INTEGER |
| **bedrooms** | Number of bedrooms. | INTEGER |
| **price** | Nightly price, numeric value without currency symbols. | NUMERIC(10,2) |
| **has_availability** | Whether listing has availability. | BOOLEAN |
| **instant_bookable** | Whether listing is instantly bookable. | BOOLEAN |
| **host_cid** | Host identifier. | INTEGER |
| **host_name** | Host name. | VARCHAR |
| **host_response_time** | Host response time. | VARCHAR |
| **host_response_rate** | Host response rate (%). | NUMERIC(5,2) |
| **host_acceptance_rate** | Host acceptance rate (%). | NUMERIC(5,2) |
| **host_is_superhost** | Whether host is a Superhost. | BOOLEAN |
| **host_has_profile_pic** | Whether host has a profile picture. | BOOLEAN |
| **host_identity_verified** | Whether host identity is verified. | BOOLEAN |
| **overall_rating** | Overall review score rating. | NUMERIC(4,2) |
| **accuracy_rating** | Accuracy rating. | NUMERIC(4,2) |
| **cleanliness_rating** | Cleanliness rating. | NUMERIC(4,2) |
| **checkin_rating** | Check-in rating. | NUMERIC(4,2) |
| **communication_rating** | Communication rating. | NUMERIC(4,2) |
| **location_rating** | Location rating. | NUMERIC(4,2) |
| **value_rating** | Value rating. | NUMERIC(4,2) |
| **license** | License information (defaulted if missing). | VARCHAR |
| **number_of_reviews** | Total number of reviews. | INTEGER |
| **number_of_reviews_ltm** | Reviews in last 12 months. | INTEGER |
| **number_of_reviews_l30d** | Reviews in last 30 days. | INTEGER |
| **estimated_occupancy_l365d** | Estimated occupancy in last year. | NUMERIC(5,2) |
| **estimated_revenue_l365d** | Estimated revenue in last year. | NUMERIC(12,2) |
| **first_review** | Date of first review. | DATE |
| **last_review** | Date of most recent review. | DATE |
| **reviews_per_month** | Avg. reviews per month. | NUMERIC(5,2) |
| **listing_neighbourhood** | Neighbourhood of listing. | VARCHAR |
| **listing_neighbourhood_cleansed** | Normalized neighbourhood. | VARCHAR |
| **neighbourhood_group_cleansed** | Higher-level neighbourhood group. | VARCHAR |
| **calculated_host_listings_count** | Total listings for host. | INTEGER |
| **calculated_host_listings_count_entire_homes** | Host’s entire-home listings. | INTEGER |
| **calculated_host_listings_count_private_rooms** | Host’s private-room listings. | INTEGER |
| **calculated_host_listings_count_shared_rooms** | Host’s shared-room listings. | INTEGER |

---

## `insideairbnb__stg_reviews`

**Staging model for InsideAirbnb listing reviews.**

### Columns

| Column | Description | Type |
|-------|-------------|------|
| **review_id** | Surrogate key (listing + review + date + geography). | VARCHAR |
| **listing_cid** | ID of listing the review belongs to. | VARCHAR |
| **review_cid** | Source review ID. | VARCHAR |
| **review_date** | Date of the review. | DATE |
| **reviewer_id** | Reviewer ID. | VARCHAR |
| **reviewer_name** | Reviewer name. | VARCHAR |
| **comments** | Review text. | VARCHAR |
| **city** | City of the listing being reviewed. | VARCHAR |
| **country** | Country. | VARCHAR |
| **extract_month** | YYYYMM batch month. | INTEGER |

---

## `insideairbnb__stg_calendar`

**Staging model for daily calendar (availability + pricing).**

### Columns

| Column | Description | Type |
|-------|-------------|------|
| **calendar_id** | Surrogate key (listing + date + geography). | VARCHAR |
| **listing_cid** | Listing identifier. | VARCHAR |
| **calendar_date** | Calendar date. | DATE |
| **is_available** | Availability flag. | BOOLEAN |
| **price** | Base price for the date. | NUMERIC(10,2) |
| **adjusted_price** | Adjusted price if provided. | NUMERIC(10,2) |
| **minimum_nights** | Minimum allowed stay. | INTEGER |
| **maximum_nights** | Maximum allowed stay. | INTEGER |
| **city** | City. | VARCHAR |
| **country** | Country. | VARCHAR |
| **extract_month** | YYYYMM batch month. | INTEGER |

---

## `googletrends__stg_trends`

**Staging model for Google Trends travel interest by city/country.**

### Columns

| Column | Description | Type |
|-------|-------------|------|
| **trend_id** | Surrogate key (date + geography). | VARCHAR |
| **trend_date** | Trend measurement date. | DATE |
| **visit_city** | Index for “visit <city>” query. | INTEGER |
| **things_to_do_in_city** | Index for “things to do in <city>” query. | INTEGER |
| **city_airbnb** | Index for “<city> airbnb” query. | INTEGER |
| **is_partial** | Whether trend is partial. | BOOLEAN |
| **city** | City. | VARCHAR |
| **country** | Country. | VARCHAR |
| **extract_month** | YYYYMM batch month. | INTEGER |

---

## `openmeteo__stg_weather`

**Staging model for Open-Meteo daily weather observations.**

### Columns

| Column | Description | Type |
|-------|-------------|------|
| **weather_id** | Surrogate key (date + geography). | VARCHAR |
| **weather_date** | Observation date. | DATE |
| **temperature_2m_max** | Daily max temperature (°C). | NUMERIC(5,2) |
| **temperature_2m_min** | Daily min temperature (°C). | NUMERIC(5,2) |
| **temperature_2m_mean** | Daily mean temperature (°C). | NUMERIC(5,2) |
| **precipitation_sum** | Total precipitation (mm). | NUMERIC(7,3) |
| **city** | City. | VARCHAR |
| **country** | Country. | VARCHAR |
| **extract_month** | YYYYMM batch month. | INTEGER |

---

## `seed__stg_holidays`

**Staging model for Azure Public Holidays seed.**

### Columns

| Column | Description | Type |
|-------|-------------|------|
| **holiday_id** | Surrogate key (country + date + name). | VARCHAR |
| **country** | Country/region. | VARCHAR |
| **holiday_date** | Holiday date. | DATE |
| **holiday_name** | Holiday name. | VARCHAR |
| **normalized_holiday_name** | Normalized holiday name. | VARCHAR |
| **is_paid_time_off** | Whether holiday is PTO. | BOOLEAN |
| **country_code** | Country code. | VARCHAR |

---

## `seed__stg_currencies`

**Staging model for country-to-currency mapping.**

### Columns

| Column | Description | Type |
|-------|-------------|------|
| **currency_id** | Surrogate key (country + currency_code). | VARCHAR |
| **country** | Country name. | VARCHAR |
| **currency_code** | 3-letter currency code. | VARCHAR |
| **currency_name** | Currency name. | VARCHAR |
