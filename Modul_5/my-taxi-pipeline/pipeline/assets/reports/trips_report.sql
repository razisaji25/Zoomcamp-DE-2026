/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: create+replace

columns:
  - name: report_date
    type: DATE
    primary_key: true
    nullable: false
    checks:
      - name: not_null

  - name: taxi_type
    type: string
    primary_key: true
    nullable: false
    checks:
      - name: not_null

  - name: payment_type_name
    type: string
    primary_key: true
    nullable: false
    checks:
      - name: not_null

  - name: trip_count
    type: BIGINT
    nullable: false
    checks:
      - name: not_null
      - name: non_negative

  - name: sum_fare_amount
    type: float
    nullable: false
    checks:
      - name: not_null
      - name: non_negative

  - name: sum_total_amount
    type: float
    nullable: false
    checks:
      - name: not_null
      - name: non_negative

  - name: avg_fare_amount
    type: float
    nullable: false
    checks:
      - name: not_null
      - name: non_negative

  - name: avg_total_amount
    type: float
    nullable: false
    checks:
      - name: not_null
      - name: non_negative

custom_checks:
  - name: trips_report_trip_count_positive_when_amounts
    query: |
      SELECT COUNT(*)
      FROM reports.trips_report
      WHERE (sum_fare_amount > 0 OR sum_total_amount > 0)
        AND trip_count = 0
    value: 0

@bruin */

SELECT
  DATE(pickup_datetime) AS report_date,
  taxi_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(fare_amount) AS sum_fare_amount,
  SUM(total_amount) AS sum_total_amount,
  AVG(fare_amount) AS avg_fare_amount,
  AVG(total_amount) AS avg_total_amount
FROM staging.trips
GROUP BY
  DATE(pickup_datetime),
  taxi_type,
  payment_type_name;