{% snapshot insideairbnb__snap_listings %}

{{
    config(
        target_schema='silver',
        unique_key='listing_id',
        strategy='check',
        check_cols=[
            'listing_name',
            'listing_description',
            'neighborhood_overview',
            'price',
            'host_name',
            'host_is_superhost',
            'room_type',
            'accommodates',
            'bathrooms',
            'beds',
            'bedrooms'
        ]
    )
}}

SELECT *
FROM {{ ref('insideairbnb__stg_listings') }}

{% endsnapshot %}