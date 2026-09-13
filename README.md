# HydroPulse — Azure Hydroponics Medallion Lakehouse

HydroPulse is an end-to-end Azure Data Engineering project that processes simulated hydroponics IoT sensor telemetry using a Medallion Lakehouse architecture.

The pipeline ingests continuous sensor events through Azure Event Hubs, processes them in Azure Databricks using PySpark and Spark SQL, stores Delta tables on ADLS Gen2, orchestrates workflows with Azure Data Factory, and serves curated Gold-layer data to Power BI.

---

## Architecture

Python IoT Simulator  
→ Azure Event Hubs  
→ Azure Data Factory  
→ Azure Databricks  
→ Bronze Delta  
→ Silver Delta  
→ Gold Delta  
→ Databricks SQL Warehouse  
→ Power BI

### Processing Pattern

- IoT devices continuously send telemetry to Azure Event Hubs.
- Azure Data Factory triggers the Databricks pipeline every 15 minutes.
- Bronze ingestion uses Spark Structured Streaming with `AvailableNow`.
- Silver performs validation, deduplication, enrichment, and anomaly detection.
- Gold produces business-ready operational and historical datasets.
- Power BI consumes Gold tables through Databricks SQL Warehouse.

---

## Technology Stack

- Microsoft Azure
- Azure Event Hubs
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Unity Catalog
- Delta Lake
- PySpark
- Spark SQL
- Python
- Power BI

---

## Medallion Architecture

### Bronze

Raw IoT telemetry is incrementally ingested from Azure Event Hubs and persisted as Delta data.

Reference farm and crop master data is also loaded into the Bronze layer.

### Silver

The Silver layer performs:

- Schema and datatype validation
- Null handling
- Physical sensor-range validation
- Event deduplication
- Packet-gap detection
- Late/out-of-order event handling
- Farm and crop master enrichment
- Crop-specific threshold evaluation
- Sensor anomaly classification
- Health and severity calculation

### Gold

The Gold layer contains three main analytical tables:

#### `gold_current_status`

Latest operational state for every farm and hydroponic zone.

#### `gold_alerts`

Historical and active sensor alert episodes.

#### `gold_daily_kpi`

Daily farm-zone KPIs including health score, anomaly rate, packet-gap rate, sensor averages, and actuator runtime.

---

## Repository Structure

```text
Hydroponics/
│
├── simulator/
│   └── hydropulse_simulator.py
│
├── databricks/
│   ├── 02_bronze_master_ingestion.py
│   ├── 03_bronze_iot_ingestion.py
│   ├── 04_silver_iot_transformation.py
│   ├── 05_gold_current_status.py
│   ├── 06_gold_alerts.py
│   ├── 07_gold_daily_kpi.py
│   ├── 08_pipeline_validation.py
│   └── job-config/
│       └── hydropulse_medallion_pipeline.json
│
├── sample-data/
│   └── farm_crop_master.csv
│
├── sql/
│   ├── gold_current_status_ddl.sql
│   ├── gold_alerts_ddl.sql
│   └── gold_daily_kpi_ddl.sql
│
├── docs/
│   └── architecture.png
│
├── powerbi/
│   └── README.md
│
├── .env.example
├── .gitignore
└── README.md