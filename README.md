## 📊 BazaarVoice – End-to-End Azure Data Engineering Project

### 📌 Project Overview

This project demonstrates the design and implementation of an end-to-end Azure Data Engineering pipeline focused on handling multiple tables with large volumes of daily incremental data using a production-style, metadata-driven architecture.

The solution ingests data from an on-premises SQL Server into Azure, processes it through layered storage (Bronze → Silver), and delivers analytics-ready datasets optimized for downstream business consumption.

<img width="1920" height="930" alt="Trigger successfull Run" src="https://github.com/user-attachments/assets/6a76f60b-c5b9-4de8-a838-1bf2b15f6f78" />


### 🎯 Business Objective

Enable scalable ingestion of frequently updated on-prem data

Handle daily incremental loads efficiently

Avoid hardcoding by using metadata-driven pipelines

Deliver clean, reliable, and analytics-ready data

Maintain ingestion state using watermark-based processing

### 🏗️ High-Level Architecture

On-Prem SQL Server
        |
        | (Self-Hosted Integration Runtime)
        v
Azure Synapse Analytics / Azure Data Factory
        |
        v
ADLS Gen2 (Bronze Layer - Raw Data)
        |
        v
Databricks (Transformations)
        |
        v
ADLS Gen2 (Silver Layer - Delta Tables)

### 🔄 Data Ingestion Strategy

🔹 Source

On-premises SQL Server

Multiple tables with high-volume daily updates

🔹 Connectivity

Self-Hosted Integration Runtime configured for secure on-prem to cloud communication

🔹 Ingestion Pattern

Incremental ingestion using watermarks

Controlled by a metadata control table stored in Azure SQL Database

### 🧠 Metadata-Driven Pipeline Design

The ingestion pipeline is fully dynamic and metadata-driven.

#### Pipeline Flow:

  Lookup Activity

  Reads active table metadata from the control table

  ForEach Activity

  Iterates over multiple source tables dynamically

  Copy Activity

  Ingests only new and updated records into ADLS Gen2 (Bronze layer)

  Databricks Notebook

  Transforms Bronze data into clean, curated Silver datasets

  Stored Procedure

  Updates last_ingestion_date to maintain incremental state

This design allows new tables to be onboarded without pipeline code changes.

#### 🥉 Bronze Layer (Raw Data)

Stores raw, unmodified data

Partitioned by ingestion date

Acts as a replay and audit layer

#### 🥈 Silver Layer (Curated Data)

Processed using Databricks notebooks

Data is:

Cleaned

Standardized

Deduplicated

Stored in Delta Lake format

Optimized for analytics and reporting

Why Delta Lake?

ACID transactions

Schema enforcement

Reliable incremental processing

Support for future enhancements like SCD

### 🛠️ Technologies Used

| Category       | Tools                                       |
| -------------- | ------------------------------------------- |
| Data Ingestion | Azure Synapse Analytics, Azure Data Factory |
| Storage        | Azure Data Lake Storage Gen2                |
| Processing     | Databricks                                  |
| Format         | Delta Lake                                  |
| Metadata Store | Azure SQL Database                          |
| Source         | SQL Server (On-Prem)                        |
| Connectivity   | Self-Hosted Integration Runtime             |

### 🚀 Key Learnings & Outcomes

Built production-style incremental pipelines

Implemented metadata-driven ingestion frameworks

Gained hands-on experience with on-prem to cloud integration

Applied Bronze → Silver layered architecture

Used Delta Lake for reliable, analytics-ready datasets

Automated ingestion state management using control tables

### 🔖 Tags

Azure Data Engineering · Incremental Ingestion · Databricks · Delta Lake · Synapse Analytics · ADLS Gen2 · Metadata Driven Pipelines
