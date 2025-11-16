# Global BnB Data Platform
> ## Note
> This project is a Work in Progress. 
> Some design choices in this project were made intentionally for learning purposes.

## Overview
The Global BnB Data Platform is a cloud-native data engineering project designed to demonstrate a modern ELT architecture using Apache Airflow, dbt, Google Cloud Storage (GCS), and Snowflake.
This project aims to build an automated and scalable pipeline for ingesting, transforming, and warehousing Airbnb datasets across multiple cities (and countries). It focuses on data pipeline orchestration, data modeling, and warehouse design, rather than analytics or visualization.

## Technology Stack
- ![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue) - Data Extraction
- ![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white) - Pipeline Orchestration
- ![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white) - Data Transformation
- ![Google Cloud Storage (GCS)](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white) - Cloud Storage
- ![Snowflake](https://img.shields.io/badge/snowflake-%2329B5E8.svg?style=for-the-badge&logo=snowflake&logoColor=white) - Data Warehouse

## Objectives
- Develop a fully automated ELT pipeline from raw data ingestion to warehouse loading.
- Implement Airflow DAGs for end-to-end orchestration and scheduling.
- Use dbt to define, document, and version-control SQL-based data transformations.
- Leverage Snowflake as the central cloud data warehouse for storage and modeling.
- Incorporate GCS as the external staging layer for raw data.
- Follow modular, maintainable, and production-ready data engineering design principles.
  
## Data Architecture
![Data Architecture](https://github.com/santoshsaranyan/global-bnb-data-platform/blob/main/images/GlobalBnBDataPlatformImage.png)
### Ingestion and Staging:
Raw Airbnb datasets, weather data and tourism trends data will be ingested and stored in Google Cloud Storage (GCS) for durability and scalability.
### Orchestration:
Apache Airflow will manage the end-to-end data flow through scheduled DAGs, handling ingestion, transformation, and loading tasks.
### Transformation:
dbt will perform data cleaning, modeling, and transfomation using SQL, following the Medallion architecture pattern (Bronze → Silver → Gold).
### Warehouse:
Snowflake will serve as the primary data warehouse, hosting production-ready tables optimized for downstream use cases.

## Source for the Data

#### AirBnB Data:
https://insideairbnb.com

#### Weather Data:
https://open-meteo.com

#### City Tourism Trends Data:
https://trends.google.com/trends (via PyTrends Package)

#### Public Holidays (dbt Seed):
https://learn.microsoft.com/en-us/azure/open-datasets/dataset-public-holidays?tabs=azureml-opendatasets 

> ## Note
> Although this project draws from the same underlying Airbnb dataset as the Boston BnB Insights App, it is a complete re-architecture of that work. The Global BnB Data Platform expands the scope beyond Boston to multiple cities and countries and is built with a modern ELT stack, leveraging Airflow, dbt, Snowflake, and GCS to enable scalable data pipelines and proper cloud-based warehousing.


## Code Author
Santosh Saranyan

https://www.linkedin.com/in/santosh-saranyan/
