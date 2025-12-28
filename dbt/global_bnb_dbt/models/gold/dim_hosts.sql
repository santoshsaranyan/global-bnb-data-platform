WITH source_data AS (
    SELECT 
    {{ dbt_utils.generate_surrogate_key([
            'city',
            'country',
            'host_cid',
        ]) }} AS host_id,
        city,
        country,
        host_cid,
        host_name,
        host_since,
        host_response_time,
        host_response_rate,
        host_acceptance_rate,
        host_is_superhost,
        host_total_listings_count,
        host_identity_verified,
        host_has_profile_pic,
        host_neighbourhood
        FROM {{ source('silver', 'insideairbnb__stg_hosts') }}
),
dim_hosts AS (
    SELECT 
        host_id,
        host_cid,
        host_name,
        host_since,
        host_response_time,
        host_response_rate,
        host_acceptance_rate,
        host_is_superhost,
        host_total_listings_count,
        host_identity_verified,
        host_has_profile_pic,
        host_neighbourhood
    FROM source_data
)

SELECT * FROM dim_hosts