# Bronze Data Catalog – Raw Sources

This catalog documents all **raw data sources** in the **bronze layer** of the Global BnB Data Warehouse.  
Bronze tables represent **source-aligned, lossless ingestions** with minimal structural changes and no business logic applied.
All columns are of the STRING data type.

---

## `insideairbnb__raw_listings`

**Raw InsideAirbnb listings data for all cities.**  
Data is ingested directly from InsideAirbnb source files and reflects the source schema prior to cleaning, typing, or enrichment.

### Columns

| Column | Description |
|------|------------|
| **id** | Unique listing identifier assigned by InsideAirbnb. |
| **listing_url** | URL linking to the listing’s page on the InsideAirbnb platform. |
| **scrape_id** | Identifier for the web scrape batch in which this listing record was collected. |
| **last_scraped** | Timestamp indicating when the listing was last scraped from the source. |
| **source** | Source platform from which the listing data was obtained. |
| **name** | Title or name of the listing as provided by the host. |
| **description** | Full textual description of the listing written by the host. |
| **neighborhood_overview** | Host-provided description of the surrounding neighbourhood. |
| **picture_url** | URL of the primary image associated with the listing. |
| **host_id** | Unique identifier of the host who owns the listing. |
| **host_url** | URL linking to the host’s profile page. |
| **host_name** | Display name of the host. |
| **host_since** | Date when the host joined the platform. |
| **host_location** | Location of the host as provided in their profile. |
| **host_about** | Free-text biography or description written by the host. |
| **host_response_time** | Reported response time of the host. |
| **host_response_rate** | Percentage string indicating how often the host responds to inquiries. |
| **host_acceptance_rate** | Percentage string indicating how often the host accepts booking requests. |
| **host_is_superhost** | Flag indicating whether the host is designated as a Superhost (t/f). |
| **host_thumbnail_url** | URL of the host’s thumbnail profile image. |
| **host_picture_url** | URL of the host’s full-size profile image. |
| **host_neighbourhood** | Neighbourhood associated with the host’s location. |
| **host_listings_count** | Number of active listings owned by the host. |
| **host_total_listings_count** | Total number of listings (active and inactive) owned by the host. |
| **host_verifications** | List of verification methods completed by the host. |
| **host_has_profile_pic** | Flag indicating whether the host has a profile picture (t/f). |
| **host_identity_verified** | Flag indicating whether the host’s identity has been verified (t/f). |
| **neighbourhood** | Neighbourhood name associated with the listing. |
| **neighbourhood_cleansed** | Normalized neighbourhood name provided by InsideAirbnb. |
| **neighbourhood_group_cleansed** | Higher-level neighbourhood or district grouping. |
| **latitude** | Latitude coordinate of the listing location. |
| **longitude** | Longitude coordinate of the listing location. |
| **property_type** | Type of property being listed. |
| **room_type** | Type of room offered. |
| **accommodates** | Maximum number of guests the listing can accommodate. |
| **bathrooms** | Number of bathrooms provided for the listing. |
| **bathrooms_text** | Textual description of bathroom availability and type. |
| **beds** | Number of beds available in the listing. |
| **bedrooms** | Number of bedrooms available in the listing. |
| **amenities** | List of amenities offered by the listing. |
| **price** | Nightly price of the listing including currency symbols. |
| **minimum_nights** | Minimum number of nights required for a booking. |
| **maximum_nights** | Maximum number of nights allowed for a booking. |
| **minimum_minimum_nights** | Minimum observed minimum nights value across calendar dates. |
| **maximum_minimum_nights** | Maximum observed minimum nights value across calendar dates. |
| **minimum_maximum_nights** | Minimum observed maximum nights value across calendar dates. |
| **maximum_maximum_nights** | Maximum observed maximum nights value across calendar dates. |
| **minimum_nights_avg_ntm** | Average minimum nights requirement over the next twelve months. |
| **maximum_nights_avg_ntm** | Average maximum nights requirement over the next twelve months. |
| **calendar_updated** | Timestamp or text indicating when the listing calendar was last updated. |
| **has_availability** | Flag indicating whether the listing has availability (t/f). |
| **availability_30** | Number of available days in the next 30 days. |
| **availability_60** | Number of available days in the next 60 days. |
| **availability_90** | Number of available days in the next 90 days. |
| **availability_365** | Number of available days in the next 365 days. |
| **calendar_last_scraped** | Date when the calendar data was last scraped. |
| **number_of_reviews** | Total number of reviews received by the listing. |
| **number_of_reviews_ltm** | Number of reviews received in the last twelve months. |
| **number_of_reviews_l30d** | Number of reviews received in the last 30 days. |
| **availability_eoy** | Number of available days remaining until the end of the year. |
| **number_of_reviews_ly** | Number of reviews received in the previous year. |
| **estimated_occupancy_l365d** | Estimated occupancy rate over the last 365 days. |
| **estimated_revenue_l365d** | Estimated revenue generated over the last 365 days. |
| **first_review** | Date of the first recorded review for the listing. |
| **last_review** | Date of the most recent review for the listing. |
| **review_scores_rating** | Overall review score provided by guests. |
| **review_scores_accuracy** | Guest rating for accuracy. |
| **review_scores_cleanliness** | Guest rating for cleanliness. |
| **review_scores_checkin** | Guest rating for check-in experience. |
| **review_scores_communication** | Guest rating for host communication. |
| **review_scores_location** | Guest rating for location. |
| **review_scores_value** | Guest rating for value for money. |
| **license** | License or registration information provided for the listing. |
| **instant_bookable** | Flag indicating whether the listing can be booked instantly (t/f). |
| **calculated_host_listings_count** | Calculated total number of listings owned by the host. |
| **calculated_host_listings_count_entire_homes** | Calculated number of entire-home listings owned by the host. |
| **calculated_host_listings_count_private_rooms** | Calculated number of private-room listings owned by the host. |
| **calculated_host_listings_count_shared_rooms** | Calculated number of shared-room listings owned by the host. |
| **reviews_per_month** | Average number of reviews received per month. |
| **city** | City associated with the listing. |
| **country** | Country associated with the listing. |
| **extract_month** | YYYYMM batch identifier indicating when the data was ingested. |

---

## `insideairbnb__raw_reviews`

**Raw InsideAirbnb listing reviews for all cities.**

### Columns

| Column | Description |
|------|------------|
| **listing_id** | Identifier of the listing to which the review belongs. |
| **id** | Unique review identifier assigned by InsideAirbnb. |
| **date** | Date when the review was submitted by the guest. |
| **reviewer_id** | Unique identifier of the reviewer. |
| **reviewer_name** | Display name of the reviewer. |
| **comments** | Free-text review comments written by the guest. |
| **city** | City associated with the reviewed listing. |
| **country** | Country associated with the reviewed listing. |
| **extract_month** | YYYYMM batch identifier indicating when the review data was ingested. |

---

## `insideairbnb__raw_calendar`

**Raw InsideAirbnb daily calendar availability and pricing data.**

### Columns

| Column | Description |
|------|------------|
| **listing_id** | Identifier of the listing to which this calendar record applies. |
| **date** | Calendar date for availability and pricing. |
| **available** | Availability flag from the source (t/f). |
| **price** | Nightly base price including currency symbols. |
| **adjusted_price** | Adjusted nightly price including currency symbols. |
| **minimum_nights** | Minimum nights required for a booking. |
| **maximum_nights** | Maximum nights allowed for a booking. |
| **city** | City associated with the listing. |
| **country** | Country associated with the listing. |
| **extract_month** | YYYYMM batch identifier indicating ingestion month. |

---

## `openmeteo__raw_weather`

**Raw daily weather observations sourced from Open-Meteo.**

### Columns

| Column | Description |
|------|------------|
| **date** | Date of the weather observation. |
| **temperature_2m_max** | Daily maximum temperature at 2 meters (°C). |
| **temperature_2m_min** | Daily minimum temperature at 2 meters (°C). |
| **temperature_2m_mean** | Daily mean temperature at 2 meters (°C). |
| **precipitation_sum** | Total daily precipitation (mm). |
| **city** | City for which the weather data applies. |
| **country** | Country for which the weather data applies. |
| **extract_month** | YYYYMM batch identifier indicating ingestion month. |

---

## `googletrends__raw_trends`

**Raw Google Trends interest-over-time data for travel-related search queries.**

### Columns

| Column | Description |
|------|------------|
| **date** | Date corresponding to the Google Trends measurement. |
| **visit_city** | Interest index (0–100) for the query “visit <city>”. |
| **things_to_do_in_city** | Interest index (0–100) for the query “things to do in <city>”. |
| **city_airbnb** | Interest index (0–100) for the query “<city> airbnb”. |
| **isPartial** | Flag indicating whether the data point is partial or incomplete (True/False as text). |
| **city** | City associated with the search interest. |
| **country** | Country associated with the search interest. |
| **extract_month** | YYYYMM batch identifier indicating ingestion month. |
