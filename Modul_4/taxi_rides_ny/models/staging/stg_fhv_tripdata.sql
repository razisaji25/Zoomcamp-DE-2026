with source as (

    select * 
    from {{ source('raw', 'fhv_tripdata') }}

),

renamed as (

    select
        -- identifiers
        cast(dispatching_base_num as varchar) as dispatching_base_num,
        cast(affiliated_base_number as varchar) as affiliated_base_number,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- locations
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- flags
        cast(sr_flag as varchar) as sr_flag

    from source

    -- basic data quality rule
    where dispatching_base_num is not null

)

select * from renamed

{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01'
  and pickup_datetime < '2019-02-01'
{% endif %}
