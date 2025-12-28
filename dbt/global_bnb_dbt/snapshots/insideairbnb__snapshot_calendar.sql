{% snapshot insideairbnb__snap_calendar %}

{{
    config(
        target_schema='silver',
        unique_key='calendar_id',
        strategy='check',
        check_cols=[
            'is_available',
            'price',
            'adjusted_price',
            'minimum_nights',
            'maximum_nights'
        ]
    )
}}

SELECT *
FROM {{ ref('insideairbnb__stg_calendar') }}

{% endsnapshot %}
