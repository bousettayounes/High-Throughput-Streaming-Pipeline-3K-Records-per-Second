# High-Throughput Streaming Pipeline: 3K Data Per-Sec

This project implements a real-time data pipeline designed for seamless data ingestion, processing, and delivery. 
The architecture leverages Apache Kafka for event streaming and Apache Spark for real-time data aggregation and processing. 
The pipeline integrates with a PostgreSQL database to store the processed data, enabling Business Intelligence (BI) teams to create dynamic, real-time visualizations.

# In this project, I have used the following technologies:
- Apache Kafka: For streaming and messaging.
- Confluent Schema Registry: To manage Avro schemas for Kafka topics.
- Apache Spark: For real-time data processing and aggregation.
- PostgreSQL: For persistent storage and integration with BI tools.

# Key Features
Event Streaming:
Apache Kafka is used as the backbone for streaming events, with schema validation ensured by the Confluent Schema Registry.

- Real-Time Data Processing:
Apache Spark processes streaming data, aggregates it, and outputs the results to:

- Kafka Topics: For further consumption by downstream systems.
PostgreSQL: Processed data is stored for BI and reporting purposes.
Real-Time BI Integration:
The PostgreSQL database provides a reliable and up-to-date data source for BI tools, ensuring real-time visual updates.

# Use Case
This pipeline is ideal for scenarios requiring real-time data analysis, such as monitoring user activity, generating reports, or creating dynamic dashboards.

----------------------------------------------------------------------------------------------------------------------------------------------
![Sys_architecture drawio (2)](https://github.com/user-attachments/assets/b0349f37-f7d6-4114-80be-bf96286508b9)
----------------------------------------------------------------------------------------------------------------------------------------------
