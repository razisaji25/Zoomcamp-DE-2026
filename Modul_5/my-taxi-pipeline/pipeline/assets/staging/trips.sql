/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: taxi_type
    type: string
    description: Taxi type (yellow or green) from ingestion
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Normalized pickup time (tpep or lpep)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Normalized dropoff time (tpep or lpep)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: payment_type_id
    type: integer
    description: TLC payment type code (join key to lookup)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human-readable payment method from lookup
    nullable: false
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: Time-and-distance fare in USD
    checks:
      - name: not_null
      - name: non_negative
  - name: total_amount
    type: float
    description: Total charged to passenger
    checks:
      - name: not_null
      - name: non_negative
  - name: extracted_at
    type: timestamp
    description: Timestamp when this batch was extracted (lineage)

custom_checks:
  - name: staging_trips_no_duplicate_key
    description: After dedup, no two rows share the same (taxi_type, pickup_datetime, dropoff_datetime, payment_type_id, fare_amount).
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT (taxi_type, pickup_datetime, dropoff_datetime, payment_type_id, fare_amount))
      FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 0

@bruin */

-- Staging: clean and normalize schema, join payment lookup, deduplicate by (taxi_type, pickup, dropoff, payment, fare, total) keeping latest extracted_at.
-- Filter by time window so time_interval strategy only inserts rows for the run's window.

WITH normalized AS (
  SELECT
    taxi_type,
    COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) AS pickup_datetime,
    COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) AS dropoff_datetime,
    payment_type,
    fare_amount,
    total_amount,
    extracted_at
  FROM ingestion.trips
  WHERE COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) >= '{{ start_datetime }}'
    AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) < '{{ end_datetime }}'
),
with_lookup AS (
  SELECT
    n.taxi_type,
    n.pickup_datetime,
    n.dropoff_datetime,
    n.payment_type AS payment_type_id,
    p.payment_type_name,
    n.fare_amount,
    n.total_amount,
    n.extracted_at
  FROM normalized n
  INNER JOIN ingestion.payment_lookup p ON n.payment_type = p.payment_type_id
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY taxi_type, pickup_datetime, dropoff_datetime, payment_type_id, fare_amount, total_amount
      ORDER BY extracted_at DESC
    ) AS rn
  FROM with_lookup
)
SELECT
  taxi_type,
  pickup_datetime,
  dropoff_datetime,
  payment_type_id,
  payment_type_name,
  fare_amount,
  total_amount,
  extracted_at
FROM ranked
WHERE rn = 1
  AND pickup_datetime IS NOT NULL
  AND dropoff_datetime IS NOT NULL
  AND fare_amount IS NOT NULL
  AND total_amount IS NOT NULL
  AND fare_amount >= 0
  AND total_amount >= 0
