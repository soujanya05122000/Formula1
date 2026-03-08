# 🏎️ Formula 1 Pipeline using Azure & Databricks

This project demonstrates how to build an **end-to-end data engineering pipeline** using **Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, and PySpark**.

The pipeline follows the **Medallion Architecture** where data is organized into **Bronze, Silver, and Gold layers**. This approach improves data quality, enables scalable processing, and produces analytics-ready datasets.

---

## 🧾 Dataset

The project processes multiple **Formula 1 datasets** that contain historical race and driver information.

Datasets used in this project include:

- **circuits.csv** – Information about race circuits  
- **drivers.json** – Driver details and nationality  
- **constructors.json** – Constructor/team information  
- **races.csv** – Race event details  
- **results.json** – Race results and finishing positions  
- **pit_stops.json** – Pit stop timing information  
- **lap_times.json** – Lap-by-lap timing data  
- **qualifying.json** – Qualifying session results  

---

## 🏗️ Pipeline Workflow

### 1️⃣ Data Ingestion (Bronze Layer)

Raw Formula 1 datasets are ingested into **Azure Data Lake Storage Gen2** using **Azure Data Factory pipelines**.

The **Bronze layer** stores the raw data exactly as received from the source system without applying major transformations.

---

### 2️⃣ Silver Layer (Cleaned Data)

Data from the Bronze layer is processed using **Azure Databricks notebooks with PySpark**.

Transformations performed include:

- Schema enforcement  
- Data cleaning  
- Data type conversion  
- Removing duplicate records  

The cleaned datasets are stored in the **Silver layer**.

---

### 3️⃣ Gold Layer (Curated Data)

The **Gold layer** contains analytics-ready datasets created from the Silver layer.

Examples include:

- Driver standings  
- Constructor standings  
- Race performance statistics  

These datasets are optimized for **analytics and reporting**.

---

## ⚙️ Tools & Technologies

- **Azure Data Factory** – Pipeline orchestration and data ingestion  
- **Azure Data Lake Storage Gen2** – Data storage  
- **Azure Databricks** – Distributed data processing  
- **PySpark** – Data transformation  
- **Unity Catalog** – Data governance and schema management  
- **Delta Lake** – Reliable lakehouse storage format  

---

## 📊 Final Output

The final output of this project is a **structured Formula 1 analytics data model** built using **Lakehouse architecture and Medallion design pattern**.

These datasets can be connected to **Power BI or Databricks SQL** for reporting and analytics.