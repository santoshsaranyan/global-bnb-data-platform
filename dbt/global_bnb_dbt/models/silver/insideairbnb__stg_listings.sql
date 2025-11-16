with stg_listings as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'city',
            'country',
            'id',
        ]) }} AS listing_id,
        id AS listing_cid,
        listing_url,
        scrape_id,
        last_scraped::DATE,
        source,
        name,
        COALESCE(description,'No information provided') as description,
        COALESCE(neighborhood_overview,'No information provided') as neighborhood_overview,
        picture_url,
        host_id,
        host_url,
        COALESCE(host_name,'Unknown') as host_name,
        host_since,
        COALESCE(host_location,'Unknown') as host_location,
        COALESCE(host_about,'No information provided') as host_about,
        NULLIF(host_response_time,'N/A') as host_response_time,
        REPLACE(NULLIF(host_response_rate,'N/A'),'%','')::NUMERIC(5,2) as host_response_rate,
        REPLACE(NULLIF(host_acceptance_rate,'N/A'),'%','')::NUMERIC(5,2) as host_acceptance_rate,
        CASE
            WHEN host_is_superhost = 't' THEN TRUE
            WHEN host_is_superhost = 'f' THEN FALSE
            ELSE NULL
        END AS host_is_superhost,
        host_thumbnail_url,
        host_picture_url,
        COALESCE(host_neighbourhood,'Unknown') as host_neighbourhood,
        host_listings_count::INTEGER,
        host_total_listings_count::INTEGER,
        host_verifications,
        CASE
            WHEN host_has_profile_pic = 't' THEN TRUE
            WHEN host_has_profile_pic = 'f' THEN FALSE
            ELSE NULL
        END AS host_has_profile_pic,
        CASE
            WHEN host_identity_verified = 't' THEN TRUE
            WHEN host_identity_verified = 'f' THEN FALSE
            ELSE NULL
        END AS host_identity_verified,
        COALESCE(neighbourhood,'Unknown') as neighbourhood,
        COALESCE(neighbourhood_cleansed,'Unknown') as neighbourhood_cleansed,
        neighbourhood_group_cleansed,
        latitude::FLOAT,
        longitude::FLOAT,
        property_type,
        room_type,
        accommodates::INTEGER,
        bathrooms::NUMERIC(3,1),
        bathrooms_text,
        bedrooms::INTEGER,
        beds::INTEGER,
        amenities,
        REGEXP_REPLACE(price,'[^0-9\.]', '', 'g')::NUMERIC(10,2) as price,
        minimum_nights::INTEGER,
        maximum_nights::INTEGER,
        minimum_minimum_nights::INTEGER,
        maximum_minimum_nights::INTEGER,
        minimum_maximum_nights::INTEGER,
        maximum_maximum_nights::INTEGER,
        minimum_nights_avg_ntm::FLOAT,
        maximum_nights_avg_ntm::FLOAT,
        calendar_updated,
        CASE
            WHEN has_availability = 't' THEN TRUE
            WHEN has_availability = 'f' THEN FALSE
            ELSE NULL
        END AS has_availability,
        availability_30::INTEGER,
        availability_60::INTEGER,
        availability_90::INTEGER,
        availability_365::INTEGER,
        calendar_last_scraped::DATE,
        number_of_reviews::INTEGER,
        number_of_reviews_ltm::INTEGER,
        number_of_reviews_l30d::INTEGER,
        availability_eoy::INTEGER,
        number_of_reviews_ly::INTEGER,
        estimated_occupancy_l365d::NUMERIC(10,2),
        estimated_revenue_l365d::NUMERIC(10,2),
        first_review::DATE,
        last_review::DATE,
        review_scores_rating::NUMERIC(4,2),
        review_scores_accuracy::NUMERIC(4,2),
        review_scores_cleanliness::NUMERIC(4,2),
        review_scores_checkin::NUMERIC(4,2),
        review_scores_communication::NUMERIC(4,2),
        review_scores_location::NUMERIC(4,2),
        review_scores_value::NUMERIC(4,2),
        COALESCE(license,'No license') as license,
        CASE
            WHEN instant_bookable = 't' THEN TRUE
            WHEN instant_bookable = 'f' THEN FALSE
            ELSE NULL
        END AS instant_bookable,
        calculated_host_listings_count::INTEGER,
        calculated_host_listings_count_entire_homes::INTEGER,
        calculated_host_listings_count_private_rooms::INTEGER,
        calculated_host_listings_count_shared_rooms::INTEGER,
        reviews_per_month::NUMERIC(6,2),
        city,
        country,
        extract_month
    from {{ source('bronze', 'insideairbnb__raw_listings') }}
)

select * from stg_listings