# Ecommerce data stream + ELT batch pipeline project and a fraud detector (Rule Based)

## Overview

This project is to demonstrate a combination of stream pipeline and ELT pipelining using Google Cloud Pub/Sub, BigQuery, dbt, Airflow and Python.

## Technology Used
- **Python** : Main programming language
- **Docker** : Containerized Airflow services and Postgres database
- **Airflow** : Orchestrating Batch pipeline
- **Postgres** : Store source data (Auto Generated Dummy Data)
- **BigQuery** : The Data Warehouse for structured data from batch pipeline
- **dbt** : Modeler for Data Warehouse (uses Medallion Architecture)
- **Pub/Sub** : Run Stream Pipeline main process

## Abstract

This project will demonstrate an ELT pipeline where data is first generated using Python and then stored to local Postgres db, and as for the orders data it will be published to stream using Pub/Sub, then the data then will be ingested by the subsciber and after that the fraud detection system will detect whether the order data is genuine or frauds. It then transferred to BigQuery via the BigQuery hook from Airflow. The data is subsequently transformed into a data warehouse structure via Dbt. The entire pipeline is orchestrated with Airflow.
The end result is transformed data stored in a data warehouse built on BigQuery and dbt, following a medallion architecture with three layers:
- Bronze/Staging layer
- Silver/Refined (Star Schema) layer
- Gold/Data Mart layer

## Archictecture
```mermaid
    flowchart TD
      Products[Products Data]
      Users[Users Data]
      Orders[Orders Data]
      Stream["Stream\n(Pub/Sub)"]
      FraudSys[Fraud\nDetector]
      DB[Database\n(Postgres)]
      BatchHourly[Airflow\n(Hourly Schedule)]
      BatchDaily[Airflow\n(Daily Schedule)]
      DWHBronze[BigQuery\nData Warehouse\nBronze Layer]
      DWHSilver[BigQuery\nData Warehouse\nSilver Layer]
      DWHGold[BigQuery\nData Warehouse\nGold Layer]
      DBT[Dbt]

      Products --> BatchHourly
      Users --> BatchHourly
      Orders --> Stream
      Stream --> FraudSys
      BatchHourly --> DB
      DB -->|Fetch Random\nProduct & User Id| Orders
      FraudSys --> |Detect genuine or fraud orders| DB
      DB --> BatchDaily
      BatchDaily --> DWHBronze
      DWHBronze --> DBT
      DBT --> DWHSilver
      DBT --> DWHGold

```

## Setup Instructions

### Prerequisites/Requirements
- You have docker installed on your device.
- You have a google cloud service account key
- You have enough hardware resource (more than 8GB RAM, and Intel i5/ Ryzen 5 or above, because if not it's going to be slow)
- (Optional) You need to have dbeaver if you want to check the postgres database

### Running the Pipeline
  1. Place your service account key on `keys` directory (create the directory first if not exist).
  2. Then run this command on your terminal:
     ```bash
     docker compose build --no-cache
     docker compose up airflow-init -d
     docker compose up -d
     ```
  3. Open your browser and put the link http://localhost:8082/, an airflow website will pop up
  4. Run the product and customer loader DAG first, after all task finished run the order loader dag
  5. Run the daily_ingest_to_bigquery DAG and wait until finished
  6. Run the dwh_silver_layer_dag and wait until finished, then repeat the same process with the dwh_gold_layer_dag 
  7. Check your google cloud console then open up BigQuery

  8. Your Data Warehouse are ready
