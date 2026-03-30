# 🚗 NHTSA Crash Dashboard (End-to-End Data Pipeline)

## 📌 Overview
This project builds an **end-to-end data pipeline** to analyze traffic accident data from the **NHTSA (National Highway Traffic Safety Administration)**.

The pipeline includes:
- Data ingestion (multi-year CSV)
- Data transformation (PySpark)
- Data warehouse (PostgreSQL)
- Visualization (Streamlit Dashboard)
- Orchestration (Docker + Cron)

---

## 🎯 Objectives
- Analyze crash patterns based on time, weather, and collision type  
- Provide an interactive dashboard for data exploration  
- Build a scalable and reusable data pipeline  

---

## 🏗️ Architecture

CSV (Raw Data)
->
PySpark (Transform)
->
Parquet (Processed Layer)
->
PostgreSQL (Data Warehouse)
->
Streamlit (Dashboard)

---

## 📂 Project Structure

```
Project_1
│
├── App/ #Streamlit_Dashboard
│   ├── dashboard.py
├── pipeline/ # ETL pipeline
│   ├── transform.py #Sprak_Partition
│   └── load.py #Load to Postgresql with DuckDB
├── data #data upload in here
│   ├── 2021 #upload manual
│       ├── accident.csv #ambil dari web "NHTSA https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/"
│       └── vehicle.csv #ambil dari web "NHTSA https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/"
│   ├── 2022 #upload manual
│       ├── accident.csv #ambil dari web "NHTSA https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/"
│       └── vehicle.csv #ambil dari web "NHTSA https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/"
│   ├── 2023 #upload manual
│       ├── accident.csv #ambil dari web "NHTSA https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/"
│       └── vehicle.csv #ambil dari web "NHTSA https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/"
│   ├── Tahun_data #upload manual
│       ├── accident.csv #ambil dari web "NHTSA https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/"
│       └── vehicle.csv #ambil dari web "NHTSA https://www.nhtsa.gov/file-downloads?p=nhtsa/downloads/FARS/"
│   └── processed #empthy folder for staging partition data from spark
├── db #data staging in DuckDB
├── docker-compose.yml
├── Dockerfile
├── requirement.txt
├── run_pipeline.sh
└── README.md
```
---

## ⚙️ Tech Stack

- 🐍 Python  
- ⚡ PySpark  
- 🐘 PostgreSQL  
- 📊 Streamlit  
- 🐳 Docker  
- ⏱️ Cron  

---

## 🚀 Getting Started

### 1. Clone Repository

```bash
git clone https://github.com/razisaji25/Zoomcamp-DE-2026.git
cd Project_1
```

### 2. Run Services (Docker)

```bash
docker compose up -d --build
```

Services:
- PostgreSQL → localhost:5433
- Streamlit → http://<YOUR-IP>:8501

### 3. Run Pipeline (manual test)

```bash
docker exec simple-pipeline bash run_pipeline.sh
```

### 4. Run Pipeline (Automation Setting)

```bash
# Step 1
crontab -e
# Step 2
0 1 * * * docker exec simple-pipeline bash /app/run_pipeline.sh
```

## 📊 Dashboard Features
🔹 Filters
- Year selection

🔹 Visualizations
- Monthly Crash Count (Bar Chart)
- Collision Type Distribution (Pie Chart)
- Harm Event Distribution (Pie Chart)
- Weather vs Harm Event (Relationship Matrix)
- Weather vs Collision Type (Relationship Matrix)

🔹 Insights
- Peak crash month
- Dominant collision type
- Weather impact analysis

## 💡 Key Learnings
- Handling multi-source relational datasets (NHTSA)
- Building scalable ETL pipelines using PySpark
- Designing a data warehouse with PostgreSQL
- Creating interactive dashboards with Streamlit
- Managing containerized workflows using Docker

### 👨‍💻 Author
Razis Aji Saputro
