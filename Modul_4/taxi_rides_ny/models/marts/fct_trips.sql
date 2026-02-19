

-- versi ringan

{{ config(
    materialized='view'
) }}

select
    trips.trip_id,
    trips.vendor_id,
    trips.service_type,
    trips.rate_code_id,

    trips.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,

    trips.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    trips.passenger_count,

    cast(trips.trip_distance as decimal(18,3)) as trip_distance,
    trips.trip_type,

    -- MUST match contract type BIGINT
    cast(
        (epoch(trips.dropoff_datetime) - epoch(trips.pickup_datetime)) / 60
        as bigint
    ) as trip_duration_minutes,

    cast(trips.fare_amount as decimal(18,3)) as fare_amount,
    cast(trips.extra as decimal(18,3)) as extra,
    cast(trips.mta_tax as decimal(18,3)) as mta_tax,
    cast(trips.tip_amount as decimal(18,3)) as tip_amount,
    cast(trips.tolls_amount as decimal(18,3)) as tolls_amount,
    cast(trips.ehail_fee as decimal(18,3)) as ehail_fee,
    cast(trips.improvement_surcharge as decimal(18,3)) as improvement_surcharge,
    cast(trips.total_amount as decimal(18,3)) as total_amount,

    trips.payment_type,
    trips.payment_type_description

from {{ ref('int_trips') }} trips

left join {{ ref('dim_zones') }} pz
    on trips.pickup_location_id = pz.location_id

left join {{ ref('dim_zones') }} dz
    on trips.dropoff_location_id = dz.location_id