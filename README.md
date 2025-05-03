# 🔄 High-Throughput Streaming Pipeline: 3K Data Per-Sec

## Overview

This project implements a comprehensive real-time streaming data pipeline designed to handle and process transactional data at scale (3,000 transactions per second). 
The solution leverages industry-standard technologies to transform raw streaming data into actionable insights through efficient processing.

---

## Business Value
The solution enables real-time decision making while maintaining performance through:
- Scalable streaming architecture that handles high-velocity data
- Schema validation ensuring data quality and consistency
- Real-time processing for immediate insights
- Integration with BI tools for live visualization

This modern streaming architecture accelerates time-to-action while providing reliable data processing, positioning your organization to respond instantly to changing business conditions.

---

## 📌 High-Level Architecture

![System Architecture](https://github.com/user-attachments/assets/14c102e3-66d1-45e4-adc1-2935a515f2b2)

- **Source Systems**:  
  - Transactional data streams

- **Pipeline Components**:
  - **Apache Kafka**: Event streaming platform
  - **Confluent Schema Registry**: Schema management and validation
  - **Apache Spark**: Real-time data processing and aggregation
  - **PostgreSQL**: Persistent storage for analytics

- **Consumption Methods**:
  - BI and Reporting Tools
  - Downstream Applications
  - Real-time Dashboards

---

## 🔄 Data Flow Architecture

The pipeline handles data through several key stages:

- **Data Ingestion**:
  - Transactional data is streamed into Kafka topics.
  - Schema Registry validates data formats using Avro schemas.

- **Processing Layer**:
  - Apache Spark consumes data from Kafka topics.
  - Real-time aggregations and transformations are applied.
  - Processed results are published back to Kafka.

- **Storage & Visualization**:
  - Aggregated data is stored in PostgreSQL.
  - BI tools connect to PostgreSQL for real-time visualization.

**Key Relationships**:
- Kafka acts as the central messaging backbone.
- Schema Registry ensures data compatibility across systems.
- Spark provides the computational power for real-time analytics.
- PostgreSQL serves as the persistent data store for BI integration.

---

## 🔥 Pipeline Components
The data pipeline is structured into three major components:
### Event Streaming (Kafka & Schema Registry)
- Transactional data is ingested into Kafka topics.
- Schema Registry ensures data format compliance.
- Tables:
  - Raw transaction topics
  - Validated transaction topics

### Real-Time Processing (Apache Spark)
- Data is processed, transformed, and aggregated in real-time.
- Streaming operations include:
  - Windowed aggregations
  - Enrichment with reference data
  - Pattern detection

### Data Storage & Visualization (PostgreSQL)
- Final processed data is stored for analysis:
  - Transaction aggregates
  - Time-series metrics
  - Business KPIs

**Highlights**:
- **Throughput**: 3,000 transactions per second
- **Latency**: Sub-second processing time
- **Usage**: Real-time dashboards, alerts, and automated actions

---

## 📂 Component Descriptions

| Component         | Description                       | Purpose | Key Capabilities                  | Scalability                                      |
| :------------ | :--------------------------------- | :---------- | :-------------------------- | :--------------------------------------------------- |
| **Apache Kafka**    | Distributed streaming platform    | Message Broker | High-throughput, fault-tolerance | Horizontal scaling through partitioning                              |
| **Schema Registry**    | Schema management service      | Data Governance | Schema evolution, compatibility checks | Centralized schema management |
| **Apache Spark**      | Distributed processing engine                | Data Processing | Stream processing, aggregation, analytics | Dynamic resource allocation         |
| **PostgreSQL**      | Relational database                | Persistent Storage | ACID transactions, SQL querying | Vertical scaling with read replicas         |

---

## 🛠️ Implementation Details

### Kafka Topic Configuration
- Partitioning strategy optimized for parallel processing
- Retention policies aligned with business requirements
- Replication factor ensuring data durability

### Spark Streaming Jobs
- Micro-batch processing with configurable checkpoint intervals
- Stateful operations for complex aggregations
- Performance tuning for optimal resource utilization

### PostgreSQL Integration
- Optimized table schemas for analytical queries
- Indexing strategy for fast lookups
- Connection pooling for efficient client management

---

## 📈 Key Features

- High-throughput ingestion handling 3,000 transactions per second
- Schema evolution management maintaining backward compatibility
- Fault-tolerant processing with at-least-once delivery semantics
- End-to-end latency optimization for real-time insights

---

## 🚀 Technologies Used

- **Apache Kafka** — Distributed streaming platform
- **Confluent Schema Registry** — Schema management service
- **Apache Spark** — Distributed processing engine
- **PostgreSQL** — Relational database for persistent storage
