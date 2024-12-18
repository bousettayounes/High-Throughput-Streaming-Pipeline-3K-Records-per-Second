# High-Throughput Streaming Pipeline: 3K Data Per-Sec

This project demonstrates a real-time streaming data pipeline designed to handle and process transactional data efficiently.
The pipeline incorporates modern tools and technologies like Kafka, Schema Registry, Spark, and PostgreSQL to process, aggregate, and store data for real-time updates and analysis.

# In this project, I have used the following technologies:
- Apache Kafka: For streaming and messaging.
- Confluent Schema Registry: To manage Avro schemas for Kafka topics.
- Apache Spark: For real-time data processing and aggregation.
- PostgreSQL: For persistent storage and integration with BI tools.

# Pipeline Overview
- Event Streaming with Kafka
- Transactional data is streamed into Kafka topics.
- Schema validation is managed using the Confluent Schema Registry to ensure consistency.
- Data Processing with Spark
 
# Key Features
- Scalable Event Streaming: Kafka topics efficiently handle large volumes of incoming transactional events.
- Schema Validation: Confluent Schema Registry ensures data consistency and compliance across the pipeline.
- Real-Time Processing: Spark provides real-time data aggregation and processing capabilities.
- Actionable Insights: Aggregated data is published back to Kafka for downstream consumers.
- Live Data Visualization: PostgreSQL serves as a data source for BI tools, enabling real-time dashboards and reporting.

# Use Case
This pipeline simulates the processing of financial transaction data, enabling:
- Real-time analytics of transaction trends and patterns.
- Real-time updates for BI dashboards.
- Support for business decision-making through actionable insights.
----------------------------------------------------------------------------------------------------------------------------------------------
![Sys_architecture drawio](https://github.com/user-attachments/assets/14c102e3-66d1-45e4-adc1-2935a515f2b2)
----------------------------------------------------------------------------------------------------------------------------------------------
