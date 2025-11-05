# Global BnB Data Platform
> ## Note
> This project is a Work in Progress

## Overview
The Global BnB Data Platform is a cloud-native data engineering project designed to demonstrate a modern ELT architecture using Apache Airflow, dbt, Google Cloud Storage (GCS), and Snowflake.
This project aims to build an automated and scalable pipeline for ingesting, transforming, and warehousing Airbnb datasets across multiple cities (and countries). It focuses on data pipeline orchestration, data modeling, and warehouse design, rather than analytics or visualization.

## Objectives
- Develop a fully automated ELT pipeline from raw data ingestion to warehouse loading.
- Implement Airflow DAGs for end-to-end orchestration and scheduling.
- Use dbt to define, document, and version-control SQL-based data transformations.
- Leverage Snowflake as the central cloud data warehouse for storage and modeling.
- Incorporate GCS as the external staging layer for raw data.
- Follow modular, maintainable, and production-ready data engineering design principles.
  
## Planned Architecture
### Ingestion and Staging:
Raw Airbnb datasets will be ingested and stored in Google Cloud Storage (GCS) for durability and scalability.
### Orchestration:
Apache Airflow, hosted on DigitalOcean, will manage the end-to-end data flow through scheduled DAGs, handling ingestion, transformation, and loading tasks.
### Transformation:
dbt will perform data cleaning, modeling, and transfomation using SQL, following the Medallion architecture pattern (Bronze → Silver → Gold).
### Warehouse:
Snowflake will serve as the primary data warehouse, hosting production-ready tables optimized for downstream use cases.

## Technology Stack
- **Language**: Python, SQL
- **Pipeline Orchestration**: Apache Airflow (hosted on DigitalOcean, containerized with Docker)
- **Data Transformation**: dbt
- **Data Warehouse**: Snowflake
- **Cloud Storage**: Google Cloud Storage (GCS)

## Source for the Dataset
https://insideairbnb.com

> ## Note
> Although this project draws from the same underlying Airbnb dataset as the Boston BnB Insights App, it is a complete re-architecture of that work. The Global BnB Data Platform expands the scope beyond Boston to multiple cities and countries and is built with a modern ELT stack, leveraging Airflow, dbt, Snowflake, and GCS to enable scalable data pipelines and proper cloud-based warehousing.


## Code Author
Santosh Saranyan

https://www.linkedin.com/in/santosh-saranyan/
