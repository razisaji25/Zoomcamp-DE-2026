# Homework 4 – DLT + DuckDB Workshop
Data Engineering Zoomcamp 2026

---

## Objective

The objective of this workshop is to:

- Ingest NYC Taxi data from a REST API using DLT
- Load the data into DuckDB
- Perform analytical queries using SQL
- Answer 3 analytical questions based on the dataset

---

## Architecture Overview

REST API
   ↓
DLT (Extract & Load)
   ↓
DuckDB (Data Warehouse)
   ↓
SQL Analysis

---

## Data Source

API Endpoint:
https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api

Dataset characteristics:
- JSON format
- 10 pages
- 1000 records per page
- Total records: 10000

---

## DLT Ingestion Code

taxi_pipeline.py

```python
import dlt
import requests

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.resource(name="nyc_taxi_data", write_disposition="replace")
def nyc_taxi_data():

    page = 1
    total_rows = 0

    while True:
        print(f"Fetching page {page}...")

        response = requests.get(
            BASE_URL,
            params={"page": page},
            timeout=30
        )
        response.raise_for_status()

        data = response.json()

        # stop ketika kosong
        if not data:
            print("No more data. Stopping.")
            break

        print(f"Rows received: {len(data)}")

        total_rows += len(data)

        yield data
        page += 1

    print(f"Total rows loaded: {total_rows}")


if __name__ == "__main__":

    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi",
        refresh="drop_sources",
        progress="log",
    )

    load_info = pipeline.run(nyc_taxi_data())
    print(load_info)
```
---
# Workshop Task – Questions & Answers
Data Engineering Zoomcamp 2026

---

## Question 1  
**What is the start date and end date of the dataset?**

SQL:

```sql
SELECT 
    MIN(Trip_Pickup_DateTime) AS start_date,
    MAX(Trip_Pickup_DateTime) AS end_date
FROM nyc_taxi.nyc_taxi_data;
```
Answer:
2009-06-01 to 2009-07-01

---

## Question 2  
**What proportion of trips are paid with credit card?**

SQL:

```sql
SELECT 
  ROUND(
    100.0 * SUM(CASE WHEN Payment_Type = 'Credit' THEN 1 ELSE 0 END)
    / COUNT(*),
  2) AS credit_card_percentage
FROM nyc_taxi.nyc_taxi_data;
```
Answer:
26.66%

---

## Question 3 
**What proportion of trips are paid with credit card?**

SQL:

```sql
SELECT 
  ROUND(
    100.0 * SUM(CASE WHEN Payment_Type = 'Credit' THEN 1 ELSE 0 END)
    / COUNT(*),
  2) AS credit_card_percentage
FROM nyc_taxi.nyc_taxi_data;
```
Answer:
$6,063.41
