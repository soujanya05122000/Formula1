#🏎️ Formula 1 Data Lakehouse Pipeline using Azure & Databricks

This project demonstrates how to build an end-to-end Formula 1 data engineering pipeline using modern cloud data engineering tools such as Azure Data Factory, Azure Databricks, PySpark, Azure Data Lake Storage Gen2, and Delta Lake.

The pipeline follows the Medallion Architecture to organize data into Bronze, Silver, and Gold layers, enabling scalable data processing, improved data quality, and analytics-ready datasets.

The pipeline supports ingestion of multiple Formula 1 datasets in CSV and JSON formats, processes them using distributed computing, and transforms them into structured datasets that can be used for analytics, reporting, and performance analysis.

#🧾 What’s in the Project?

This project processes multiple Formula 1 racing datasets which include historical race and driver information.

Some of the key datasets used include:

circuits.csv – Information about Formula 1 race circuits

drivers.json – Driver details including nationality and driver code

constructors.json – Constructor/team information

races.csv – Race event details such as location and season

results.json – Race results including finishing positions

pit_stops.json – Pit stop timing information

lap_times.json – Lap-by-lap timing information

qualifying.json – Qualifying session results

These datasets are ingested and processed through the pipeline to generate analytics-ready datasets.

#🏗️ Step-by-Step Workflow
1. Data Ingestion

The raw Formula 1 datasets are stored in Azure Data Lake Storage Gen2.

An Azure Data Factory pipeline is used to orchestrate the ingestion process. The pipeline reads the raw datasets and organizes them in the Bronze layer of the data lake.

This ingestion pipeline supports loading multiple datasets and ensures structured storage within the data lake.

2. Bronze Layer – Raw Zone

The Bronze layer stores raw data exactly as received from the source.

Key characteristics of the Bronze layer:

Raw CSV and JSON files are ingested

Minimal transformation is applied

Data is stored in Azure Data Lake Storage

Maintains a historical copy of source data

The data is stored in Parquet format to improve storage efficiency and query performance.

3. Unity Catalog Setup

Unity Catalog is configured in Azure Databricks to provide centralized data governance.

The following schemas are created to organize the data layers:

Bronze schema – Stores raw ingested datasets

Silver schema – Stores cleaned and processed datasets

Gold schema – Stores curated analytics datasets

Unity Catalog ensures proper access control, metadata management, and governance for the data lakehouse.

4. Silver Layer – Cleaned Zone

The Silver layer contains cleaned and standardized data derived from the Bronze layer.

Databricks notebooks are used to process the raw data using PySpark.

Key transformations performed include:

Schema enforcement

Data type conversions

Column renaming and standardization

Handling missing values

Removing duplicate records

The processed datasets are stored in the Silver layer as Delta tables, enabling reliable and efficient data storage.

5. Gold Layer – Curated Zone

The Gold layer contains business-level datasets designed for analytics and reporting.

In this stage, data from the Silver layer is transformed into analytical models such as:

Driver standings

Constructor standings

Race performance statistics

Season-wise driver performance

PySpark transformations and aggregations are used to generate these datasets.

The final output is stored as Delta tables in the Gold layer, optimized for analytics and BI tools.

6. Job Orchestration

A Databricks job workflow is created to automate the entire data pipeline.

The job runs all the notebooks sequentially:

Bronze ingestion notebooks

Silver transformation notebooks

Gold aggregation notebooks

This allows the entire ETL process to be executed with a single trigger, enabling automated data processing.

#📊 Final Output

The final output of this project is a structured Formula 1 analytics data model built using:

Delta Lakehouse architecture

Medallion data architecture

Scalable distributed data processing

The curated datasets can be easily connected to:

Power BI dashboards

Databricks SQL analytics

Business reporting tools

These datasets enable insights into driver performance, constructor standings, and race statistics across seasons.

#🧱 Tools & Technologies

The following tools and technologies are used in this project:

Azure Data Factory – Pipeline orchestration and data ingestion

Azure Data Lake Storage Gen2 – Scalable data storage

Azure Databricks – Distributed data processing platform

PySpark – Data transformation and processing

Unity Catalog – Data governance and access management

Delta Lake – Reliable and scalable data lakehouse storage