-- -- Enrich and deduplicate trip data
-- -- Demonstrates enrichment and surrogate key generation
-- -- Note: Data quality analysis available in analyses/trips_data_quality.sql

-- with unioned as (
--     select * from {{ ref('int_trips_unioned') }}
-- ),

-- payment_types as (
--     select * from {{ ref('payment_type_lookup') }}
-- ),

-- cleaned_and_enriched as (
--     select
--         -- Generate unique trip identifier (surrogate key pattern)
--         {{ dbt_utils.generate_surrogate_key(['u.vendor_id', 'u.pickup_datetime', 'u.pickup_location_id', 'u.service_type']) }} as trip_id,

--         -- Identifiers
--         u.vendor_id,
--         u.service_type,
--         u.rate_code_id,

--         -- Location IDs
--         u.pickup_location_id,
--         u.dropoff_location_id,

--         -- Timestamps
--         u.pickup_datetime,
--         u.dropoff_datetime,

--         -- Trip details
--         u.store_and_fwd_flag,
--         u.passenger_count,
--         u.trip_distance,
--         u.trip_type,

--         -- Payment breakdown
--         u.fare_amount,
--         u.extra,
--         u.mta_tax,
--         u.tip_amount,
--         u.tolls_amount,
--         u.ehail_fee,
--         u.improvement_surcharge,
--         u.total_amount,

--         -- Enrich with payment type description
--         coalesce(u.payment_type, 0) as payment_type,
--         coalesce(pt.description, 'Unknown') as payment_type_description

--     from unioned u
--     left join payment_types pt
--         on coalesce(u.payment_type, 0) = pt.payment_type
-- )

-- select * from cleaned_and_enriched

-- -- Deduplicate: if multiple trips match (same vendor, second, location, service), keep first
-- qualify row_number() over(
--     partition by vendor_id, pickup_datetime, pickup_location_id, service_type
--     order by dropoff_datetime
-- ) = 1

-- versi ringan
{{ config(materialized='view') }}

-- Optimized, contract-safe, precision-safe version
-- Lightweight for DuckDB local execution

select distinct on (
    u.vendor_id,
    u.pickup_datetime,
    u.pickup_location_id,
    u.service_type
)

    -- Surrogate key (contract-compatible)
    cast(
        hash(
            u.vendor_id,
            u.pickup_datetime,
            u.pickup_location_id,
            u.service_type
        ) as varchar
    ) as trip_id,

    -- Identifiers
    cast(u.vendor_id as integer) as vendor_id,
    u.service_type,
    cast(u.rate_code_id as integer) as rate_code_id,

    -- Location IDs
    cast(u.pickup_location_id as integer) as pickup_location_id,
    cast(u.dropoff_location_id as integer) as dropoff_location_id,

    -- Timestamps
    u.pickup_datetime,
    u.dropoff_datetime,

    -- Trip details
    u.store_and_fwd_flag,
    cast(u.passenger_count as integer) as passenger_count,
    cast(u.trip_type as integer) as trip_type,

    -- Precision-safe metrics
    cast(u.trip_distance as decimal(18,2)) as trip_distance,
    cast(u.fare_amount as decimal(18,2)) as fare_amount,
    cast(u.extra as decimal(18,2)) as extra,
    cast(u.mta_tax as decimal(18,2)) as mta_tax,
    cast(u.tip_amount as decimal(18,2)) as tip_amount,
    cast(u.tolls_amount as decimal(18,2)) as tolls_amount,
    cast(u.ehail_fee as decimal(18,2)) as ehail_fee,
    cast(u.improvement_surcharge as decimal(18,2)) as improvement_surcharge,
    cast(u.total_amount as decimal(18,2)) as total_amount,

    -- Payment enrichment
    cast(coalesce(u.payment_type, 0) as integer) as payment_type,
    coalesce(pt.description, 'Unknown') as payment_type_description

from {{ ref('int_trips_unioned') }} u

left join {{ ref('payment_type_lookup') }} pt
    on coalesce(u.payment_type, 0) = pt.payment_type

order by
    u.vendor_id,
    u.pickup_datetime,
    u.pickup_location_id,
    u.service_type,
    u.dropoff_datetime

