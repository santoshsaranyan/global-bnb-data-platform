WITH host_descriptions AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key([
            'city',
            'country',
            'host_cid',
        ]) }} AS host_id,
        host_cid,
        host_name,
        host_about AS host_description,
        INITCAP(city) AS city,
        INITCAP(country) AS country,
    FROM {{ source('silver', 'insideairbnb__stg_listings') }}
)

SELECT * FROM host_descriptions

