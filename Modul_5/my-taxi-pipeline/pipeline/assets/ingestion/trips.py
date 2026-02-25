"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

depends:
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
  - name: extracted_at
    type: timestamp
  - name: tpep_pickup_datetime
    type: timestamp
  - name: tpep_dropoff_datetime
    type: timestamp
  - name: lpep_pickup_datetime
    type: timestamp
  - name: lpep_dropoff_datetime
    type: timestamp
  - name: payment_type
    type: integer
  - name: fare_amount
    type: float
  - name: total_amount
    type: float
@bruin"""

import os
import json
from datetime import datetime

import pandas as pd


def materialize():
    """
    Ingest NYC TLC taxi trip data from the public Parquet endpoint.
    Uses BRUIN_START_DATE, BRUIN_END_DATE and taxi_types (from BRUIN_VARS)
    to build URLs, fetches each file, adds taxi_type and extracted_at, and returns
    one concatenated DataFrame (append-only; duplicates handled in staging).
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    start_date = os.environ.get("BRUIN_START_DATE")  # YYYY-MM-DD
    end_date = os.environ.get("BRUIN_END_DATE")
    if not start_date or not end_date:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE must be set")
    vars_json = os.environ.get("BRUIN_VARS", "{}")
    try:
        vars_data = json.loads(vars_json)
    except json.JSONDecodeError:
        vars_data = {}
    taxi_types = vars_data.get("taxi_types", ["yellow", "green"])
    if isinstance(taxi_types, str):
        taxi_types = [taxi_types]

    extracted_at = datetime.utcnow()

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    year_months = []
    y, m = start.year, start.month
    end_y, end_m = end.year, end.month
    while (y, m) <= (end_y, end_m):
        year_months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    frames = []
    for taxi_type in taxi_types:
        for (year, month) in year_months:
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = base_url + filename
            try:
                df = pd.read_parquet(url)
            except Exception as e:
                # Skip missing months (e.g. future or not yet published)
                continue
            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            # Convert datetime columns to ISO strings to avoid PyArrow tzdata lookup on Windows
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].astype(str)
            frames.append(df)

    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    for col in result.columns:
        if pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = result[col].astype(str)
    return result


