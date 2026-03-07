# 🚀 Data Engineering Zoomcamp 2026 – Apache Spark Homework

This repository contains the implementation for **Module 6 – Apache Spark** from the **Data Engineering Zoomcamp 2026**.

The objective of this module is to process **NYC Yellow Taxi Trip Data** using **PySpark** and perform several analytical queries.

---

# 📦 Technologies

This project uses the following tools:

- Python
- Apache Spark / PySpark
- uv (Python package manager)
- Parquet
- Git / GitHub

---

# 📊 Dataset

NYC Yellow Taxi dataset – **November 2025**

Download dataset:

https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet

Taxi zone lookup:

https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

⚠️ Datasets are **not included in this repository** to keep the repository lightweight.

---

# ⚙️ Environment Setup

Create Python environment using **uv**:

```bash
uv venv
source .venv/bin/activate
uv pip install pyspark
```

Run scripts with:

```bash
uv run python script_name.py
```

---

# 📂 Project Structure

```
Zoomcamp-DE-2026
│
├── Modul_6
│   ├── count_taxi_trips_15_nov_2025.py
│   ├── longest_trips.py
│   ├── Least_frequent_pickup_zone.py
│   ├── yellow_2025_11_spark.py #Cek size per partition
│   ├── yellow_tripdata_2025-11.parquet #downloads
│   └── taxi_zone_lookup.csv #downloads
│
├── pyproject.toml
├── uv.lock
└── README.md
```

Ignored files include:

- `.venv`
- `.parquet`
- Spark output folders
- large datasets

---

# 🧠 Homework Solutions

## Question 1 – Install Spark and PySpark

with guide in zoomcamp, spark successfully installed and verified.
then for check version with
```
uv run python -c "import pyspark; print(pyspark.__version__)"
```
Result:

```
Spark version: 4.1.1
```

---

## Question 2 – Yellow November 2025

Steps:

1. Load dataset into Spark
2. Repartition dataframe into **4 partitions**
3. Save dataframe as **Parquet**
4. running on cmd:
```bash
uv run python yellow_2025_11_spark.py
```
5. check size with
```bash
ls -lh yellow_2025_11_repartitioned
```
Average file size:
```
~25 MB
```

✅ **Answer:** 25MB

---

## Question 3 – Count Records

Number of taxi trips that **started on 15 November 2025**:
running on cmd:
```bash
uv run python count_taxi_trips_15_nov_2025.py
```
result:
```
162,604
```

---

## Question 4 – Longest Trip

Longest trip duration in the dataset:
running on cmd:
```bash
uv run python longest_trips.py
```
result:
```
90.6 hours
```

---

## Question 5 – Spark User Interface

Spark Web UI runs on:

```
http://localhost:4040
```

---

## Question 6 – Least Frequent Pickup Zone

Using Spark SQL join with taxi zone lookup.
running on cmd:
```bash
uv run python Least_frequent_pickup_zone.py
```
Result:

```
Governor's Island/Ellis Island/Liberty Island
```

---
---

# 📈 Spark UI

Spark dashboard can be accessed at:

```
http://localhost:4040
```

If running on a cloud VM, use SSH port forwarding:

```bash
ssh -L 4040:localhost:4040 ubuntu@YOUR_SERVER_IP
```

---

# 🧾 Notes

- Spark runs in **local mode**
- Repository only contains **code and configuration**

---

# 📚 References

- Data Engineering Zoomcamp
- NYC TLC Trip Record Data
- Apache Spark Documentation

---

# 👤 Author

**Razis Aji Saputro**

Data Engineering Zoomcamp 2026
