# OCP Transport Intelligence Platform 🚛📊

An enterprise-grade, end-to-end Data Engineering and Analytics platform designed for logistics and fleet transportation management at OCP Group. The platform ingests operational transport and telemetry data, processes it through a Medallion Lakehouse architecture using Apache Spark (PySpark) and MinIO, replicates data to Snowflake, transforms analytical data marts using dbt, and orchestrates the entire workflow with Apache Airflow.

---

## 🏗️ Architecture Overview

```mermaid
flowchart TD
    subgraph Operational_Layer["Operational Layer"]
        PG[(PostgreSQL\ntransports schema)]
        DataGen[Data]
        DataGen -->|data ingestion| PG
    end

    subgraph Replication_Layer["Direct Ingestion / Sync"]
        Loader[load_postgres_to_snowflake.py]
        PG --> Loader
        Loader -->|Staging Load| SF_RAW[(Snowflake\nRAW Schema)]
    end

    subgraph Lakehouse_Layer["Data Lakehouse (MinIO S3 + PySpark)"]
        Bronze[Bronze Layer\nRaw Parquet]
        Silver[Silver Layer\nCleaned & Validated Parquet]
        Quarantine[Quarantine Layer\nRejected Records]
        Gold[Gold Layer\nAggregated KPIs Parquet]

        PG -->|PySpark JDBC Extract| Bronze
        Bronze -->|PySpark Data Quality & Cleansing| Silver
        Bronze -.->|Invalid records| Quarantine
        Silver -->|Aggregations & Metric Computations| Gold
    end

    subgraph Analytics_Layer["Cloud Data Warehouse & Modeling (Snowflake + dbt)"]
        SF_RAW --> dbt_stg[dbt Staging Models\nstg_vehicle, stg_trip, etc.]
        dbt_stg --> dbt_analytics[dbt Analytics Marts\nvehicle_kpis, route_kpis,\ndriver_kpis, fleet_kpis]
    end

    subgraph Orchestration["Orchestration Engine"]
        Airflow[Apache Airflow 3.x\nocp_transport_batch_pipeline DAG]
    end

    Airflow -.-> Loader
    Airflow -.-> Bronze
    Airflow -.-> Silver
    Airflow -.-> Gold
    Airflow -.-> dbt_stg
```

---

## 🛠️ Tech Stack

| Technology | Role |
| :--- | :--- |
| **PostgreSQL 16** | Operational OLTP database storing transactions, vehicles, drivers, and trips |
| **Apache Kafka** | Real-time event streaming for vehicle telemetry and operational alerts |
| **MinIO** | S3-compatible Object Storage powering the Lakehouse (Bronze / Silver / Gold / Streaming) |
| **Apache Spark (PySpark 3.5)** | Distributed batch processing, data quality, and Structured Streaming for real-time alerts |
| **Snowflake** | Cloud Data Warehouse hosting the raw staging and analytical reporting marts |
| **dbt (Data Build Tool)** | Data modeling, transformations, testing, and schema documentation |
| **scikit-learn 1.8** | Machine learning engine for trip ETA prediction, predictive maintenance, and fuel optimization |
| **Apache Airflow 3.x** | Workflow orchestration, task dependency management, and pipeline monitoring |
| **Streamlit** | Interactive Python web dashboard visualizing Gold KPIs and live ML predictions |
| **Docker & Docker Compose** | Multi-container environment for Postgres, MinIO, and Airflow services |
| **Python & Faker** | Data generation and ETL scripts |

---

## 📂 Medallion Lakehouse Architecture

The Lakehouse layer follows the **Medallion Architecture** pattern using MinIO S3 object storage:

### 1. 🥉 Bronze Layer (`s3a://ocp-data/bronze/`)
- Ingests raw data from PostgreSQL via PySpark JDBC (`org.postgresql.Driver`).
- Overwrites and stores datasets in columnar Parquet format:
  - `vehicle`, `driver`, `route`, `trip`, `gps_event`, `fuel_transaction`, `maintenance`, `incident`.

### 2. 🥈 Silver Layer (`s3a://ocp-data/silver/`)
- Trims whitespace, standardizes casing (e.g. uppercase license plates).
- Enforces strict business validations and data quality checks (e.g., non-negative distances, valid fuel types, valid statuses).
- **Quarantine Pattern (`s3a://ocp-data/quarantine/`)**: Any records failing data quality rules are routed to the quarantine path for inspection without breaking the pipeline.

### 3. 🥇 Gold Layer (`s3a://ocp-data/gold/`)
- Aggregated, analytics-ready business datasets:
  - **`vehicle_kpis`**: Total distance, trip counts, fuel consumption, downtime, maintenance costs, efficiency.
  - **`route_kpis`**: Planned vs. actual distance, route deviation km and percentage, cargo tons delivered, average duration.
  - **`driver_kpis`**: Completed trips, total distance driven, average cargo carried, safety incident counts.
  - **`fleet_kpis`**: High-level fleet-wide health, operational utilization, total fuel expenses, incident rates.

### 4. ⚡ Streaming Layer (`s3a://ocp-data/streaming/`)
- Real-time ingestion of vehicle telemetry via **Kafka** (`vehicle_telemetry` topic).
- **Spark Structured Streaming Engine** (`spark_alert_engine.py`) processes telemetry to detect:
  - Overspeeding (`>100 km/h`), High Engine Temperatures (`>100 C`), Low Fuel Levels (`<15%`), and Unexpected Stops.
- Alerts are persisted as Parquet files to S3 and pushed back to Kafka (`vehicle_alerts` topic).

---

## ❄️ Analytics Engineering with dbt & Snowflake

The dbt project (`dbt/ocp_transport/`) models data within Snowflake across two layers:

### Staging Models (`models/staging/`)
- `stg_vehicle.sql`: Standardized vehicle dimension with valid capacity and year checks.
- `stg_driver.sql`: Driver dimension with experience and status tracking.
- `stg_route.sql`: Planned routes, origin/destination pairs, and planned distances.
- `stg_trip.sql`: Trips with calculated trip duration in minutes and validity constraints.
- `stg_gps_event.sql`: Telemetry events with geographic and speed validations.
- `stg_fuel_transaction.sql`: Fuel receipts with calculated total transaction costs.
- `stg_maintenance.sql`: Maintenance logs with downtime hours and cost tracking.
- `stg_incident.sql`: Safety incidents with severity classifications.

### Analytics Marts (`models/analytics/`)
- `vehicle_kpis.sql`: Aggregated performance, cost per vehicle, maintenance metrics.
- `route_kpis.sql`: Route deviations, efficiency, cargo throughput.
- `driver_kpis.sql`: Driver performance, completion rates, incident records.
- `fleet_kpis.sql`: Executive-level operational metrics and fleet KPIs.
- `realtime_fleet_kpis.sql`: Streaming aggregation of real-time telemetry for active vehicle states, speeds, and anomaly rates.

---

## 🔮 Machine Learning & Predictive Analytics

The platform integrates a production-grade machine learning module (`src/ml/`) trained directly on historical fleet telemetry and operational data extracted from Snowflake:

### 1. ⏱️ Smart Trip Duration & ETA Prediction
- **Algorithm**: Multi-feature `RandomForestRegressor` ensemble.
- **Features**: Route distance, cargo tonnage, vehicle payload capacity and utilization, driver experience, departure hour, day of week, route type, vehicle type.
- **Performance**: **$R^2 = 0.9653$**, **$MAE = 26.6\text{ min}$**, **$RMSE = 37.1\text{ min}$**.
- **Interactive Capability**: Real-time corridor simulator in Streamlit computing exact transit times, estimated arrival times (ETA), rush-hour delay buffers, and 95% confidence windows.

### 2. 🛡️ Fleet Predictive Maintenance & Breakdown Risk Scoring
- **Algorithm**: `RandomForestClassifier` with balanced class weighting.
- **Features**: Vehicle age, accumulated distance (km), total maintenance operations, downtime hours, incident history, fuel efficiency rate.
- **Performance**: **$\text{ROC-AUC} = 0.9923$**, **$\text{Accuracy} = 96.0\%$**, **$F1 = 0.977$**.
- **Interactive Capability**: Fleet-wide health triage scanner categorizing vehicles into `LOW`, `MEDIUM`, `HIGH`, and `CRITICAL` risk tiers, coupled with single-vehicle diagnostic drill-downs and prescriptive workshop actions.

### 3. 🌿 Fuel Consumption & Eco-Driving Optimizer
- **Algorithm**: Payload-aware ensemble regressor ($R^2 = 0.9929$, $MAE = 5.38\text{ L}$).
- **Capability**: Compares theoretical expected fuel burn vs. actual fuel consumption to assign Eco-Driving grades (`A+` to `D`), quantify carbon footprints ($\text{kg CO}_2$), and flag anomalous consumption from engine idling or fuel leakage.

---

## ⏱️ Orchestration: Airflow Batch Pipeline

The DAG `ocp_transport_batch_pipeline` orchestrates tasks in the following sequence:

```
load_postgres_to_snowflake
           │
           ▼
    bronze_ingestion
           │
           ▼
  silver_transformation
           │
  ┌────────┼────────┬────────┐
  ▼        ▼        ▼        ▼
gold_   gold_    gold_    gold_
vehicle route    driver   fleet
  └────────┬────────┴────────┘
           ▼
        dbt_run
           │
           ▼
        dbt_test
```

---

## 📁 Repository Structure

```
├── .env.example                      # Template environment variables
├── .gitignore                        # Git ignore rules for environments, logs, and data
├── docker-compose.yml                # Docker services (Postgres, MinIO, Airflow, Cloudflare)
├── requirement.txt                   # Python dependencies
├── readme.md                         # Project documentation
│
├── airflow/                          # Airflow orchestration
│   ├── dags/
│   │   └── ocp_transport_batch.py    # Main Airflow batch DAG
│   └── dockerfile                    # Custom Airflow image with Spark, dbt, and Java 17
│
├── database/                         # Database DDL & configs
│   ├── minio/
│   │   └── snowflake-readonly.json   # MinIO bucket policy for Snowflake access
│   ├── queries/
│   │   └── kpis.sql                  # Analytical SQL queries
│   └── schema/
│       └── tables.sql                # PostgreSQL DDL table schemas and constraints
│
├── dbt/                              # dbt Analytics Project
│   ├── profiles.yml                  # Snowflake connection profile
│   └── ocp_transport/
│       ├── dbt_project.yml           # dbt project configurations
│       ├── macros/
│       └── models/
│           ├── staging/              # Staging SQL models & sources
│           └── analytics/            # Marts SQL models & schema tests
│
├── docs/                             # Documentation
│   └── logical_data_model.md         # Detailed logical data model & business rules
│
├── drivers/                          # Database drivers
│   └── postgresql-42.7.12.jar        # PostgreSQL JDBC driver for Spark
│
└── src/                              # Core Python & Spark code
    ├── dashboard/                    # Streamlit Dashboard application
    │   └── app.py                    # Main dashboard script (KPIs + Real-time + ML tabs)
    ├── generate_data.py              # Synthetic data generator for PostgreSQL
    ├── load_postgres_to_snowflake.py # Direct Postgres-to-Snowflake sync
    ├── snowflake_test.py             # Snowflake connectivity test
    ├── ml/                           # Machine Learning & AI Operations
    │   ├── data_loader.py            # Feature extraction from Snowflake / local fallback
    │   ├── train.py                  # End-to-end model training pipeline
    │   ├── predictor.py              # OCPMLEngine inference service
    │   ├── verify_ml.py              # Automated test & validation script
    │   └── artifacts/                # Serialized model pipelines & metrics.json
    ├── pyspark/                      # Spark ETL & Medallion scripts
        ├── bronze_ingestion.py       # Postgres -> MinIO Bronze
        ├── silver_transformations.py # Bronze -> Silver & Quarantine
        ├── gold_vehicle_kpis.py      # Vehicle KPIs Gold mart
        ├── gold_route_kpis.py        # Route KPIs Gold mart
        ├── gold_driver_kpis.py       # Driver KPIs Gold mart
        ├── gold_fleet_kpis.py        # Fleet KPIs Gold mart
        ├── trip_analysis.py          # Ad-hoc PySpark trip analysis
        └── minio_test.py             # MinIO connectivity test
    ├── streaming/                    # Spark Structured Streaming
        └── spark_alert_engine.py     # Real-time Kafka telemetry to S3 alerts
```

---

## 🚀 Getting Started

### 1. Clone the Repository
```bash
git clone https://github.com/Yussefech31/tracking-ocp.git
cd tracking-ocp
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and fill in your Snowflake and service credentials:
```bash
cp .env.example .env
```

### 3. Launch Services with Docker Compose
```bash
docker compose up -d
```
Services available:
- **PostgreSQL**: `localhost:5433`
- **MinIO Console**: `http://localhost:9001` (User: `minio_admin`, Password: `minio_password123`)
- **MinIO S3 API**: `http://localhost:9000`
- **Airflow Webserver**: `http://localhost:8083` (User: `admin`, Password: `admin`)

### 4. Initialize Database & Generate Test Data
```bash
# Execute schema creation in PostgreSQL
docker exec -i ocp-postgres psql -U ocp_admin -d ocp_transport < database/schema/tables.sql

# Generate realistic test records
python src/generate_data.py
```

### 5. Trigger the Airflow Pipeline
Access the Airflow UI at `http://localhost:8083`, unpause the DAG `ocp_transport_batch_pipeline`, and trigger a run to execute the end-to-end ingestion, Spark medallion transformations, and dbt models.

### 6. Train Machine Learning Models
Train the trip ETA regressor, predictive maintenance classifier, and fuel optimizer:
```bash
python -m src.ml.train
# Run automated validation
python src/ml/verify_ml.py
```

### 7. Run the Streamlit Dashboard
Start the interactive dashboard to visualize the Gold KPIs, real-time alerts, and live ML simulators:
```bash
python -m streamlit run src/dashboard/app.py
```
The dashboard will be available at `http://localhost:8501`.
