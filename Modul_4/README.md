# Module 4 Homework: Analytics Engineering with dbt

In this homework, we'll use the dbt project in `04-analytics-engineering/taxi_rides_ny/` to transform NYC taxi data and answer questions by querying the models.

## Setup

1. Set up your dbt project following the [setup guide](../../../04-analytics-engineering/setup/)
2. Load the Green and Yellow taxi data for 2019-2020 and FHV trip data for 2019 into your warehouse (use static tables from [dtc github](https://github.com/DataTalksClub/nyc-tlc-data/), don't use offical tables from tlc because some values change from time to time)
3. Run `dbt build --target prod` to create all models and run tests

> **Note:** By default, dbt uses the `dev` target. You must use `--target prod` to build the models in the production dataset, which is required for the homework queries below.

After a successful build, you should have models like `fct_trips`, `dim_zones`, and `fct_monthly_zone_revenue` in your warehouse.

---

### Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- [ ] `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
- [ ] `Any model with upstream and downstream dependencies to `int_trips_unioned`
- [x] `int_trips_unioned` only
- [ ] `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)

## Answer

**int_trips_unioned only**

## Explanation

When you use the `--select` flag (or its shorthand `-s`) in dbt without any additional operators, dbt will 
exclusively run the specific model that you name.

Example:

```bash
dbt run --select int_trips_unioned
```

---

### Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

- [ ] dbt will skip the test because the model didn't change
- [X] dbt will fail the test, returning a non-zero exit code
- [ ] dbt will pass the test with a warning about the new value
- [ ] dbt will update the configuration to include the new value

## Answers:

The answer is dbt will fail the test, returning a non-zero exit code

---

### Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

- [ ] 12,998
- [ ] 14,120
- [X] 12,184
- [ ] 15,421

## Answers:
After 
```bash
run dbt run --target prod  --select +fct_monthly_zone_revenue
```

we running duckdb -ui and connect to database taxi_rides_ny.duckdb
and then running

```sql
select count(*)  from prod.fct_monthly_zone_revenue; 
```

---

### Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

- [X] East Harlem North
- [ ] Morningside Heights
- [ ] East Harlem South
- [ ] Washington Heights South

## Answers:
After 
```bash
run dbt run --target prod  --select +fct_monthly_zone_revenue
```
we running duckdb -ui and connect to database taxi_rides_ny.duckdb
and then running

```sql
select
pickup_zone,
sum(revenue_monthly_total_amount) as total_revenue
from prod.fct_monthly_zone_revenue
where service_type = 'Green'
and revenue_month >= '2020-01-01'
and revenue_month <  '2021-01-01'
group by pickup_zone
order by total_revenue desc
limit 1;
```

---

### Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

- [ ] 500,234
- [ ] 350,891
- [X] 384,624
- [ ] 421,509

## Answers:
After 
```bash
run dbt run --target prod  --select +fct_monthly_zone_revenue
```
we running duckdb -ui and connect to database taxi_rides_ny.duckdb
and then running

```sql
  SELECT 
  SUM(total_monthly_trips) AS total_trips
  FROM fct_monthly_zone_revenue
  WHERE service_type = 'Green'
  AND revenue_year = 2019
  AND revenue_month = 10;
```

---

### Question 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

- [ ] 42,084,899
- [X] 43,244,693
- [ ] 22,998,722
- [ ] 44,112,187

## Answers:
Create Source and schema setting for stg_fhv_tripdata
Create model for stg_fhv_tripdata.sql

```sql
{{ config(
    materialized='view'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'fhv_tripdata') }}

),

cleaned AS (

    SELECT

        dispatching_base_num,

        CAST(pickup_datetime AS TIMESTAMP)  AS pickup_datetime,
        CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

        CAST(pulocationid AS INTEGER) AS pickup_location_id,
        CAST(dolocationid AS INTEGER) AS dropoff_location_id,

        SR_Flag,

        'FHV' AS service_type

    FROM source
    WHERE pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND pulocationid IS NOT NULL
      AND dolocationid IS NOT NULL

)

SELECT *
FROM cleaned
```

After 
```bash
run dbt run --target prod  +stg_fhv_tripdata
``` 
we running duckdb -ui and connect to database taxi_rides_ny.duckdb
and then running

```sql
select count(*)  from prod.stg_fhv_tripdata
```

