# Overview

This project implements a **batch-based ETL data pipeline** that extracts earthquake data from a public API, cleans and transforms it, and loads the processed data into a local SQL Server database.
The pipeline is orchestrated using Apache Airflow 3.x and follows standard data engineering best practices.

The design intentionally separates raw data, processed data, and business logic, making the pipeline reliable, reproducible, and easy to extend.


# Architecture

## High-level flow:

USGS Earthquake API
    ↓
Extract (API → JSON)
    ↓
Raw Data 
    ↓
Transform (JSON → Clean CSV)
    ↓
Processed Data 
    ↓
Load (CSV → SQL Server)


# Technologies Used

* Python 3

* Apache Airflow 3.x

* Pandas

* Requests

* SQLAlchemy

* pyodbc

* Docker & Docker Compose

* Microsoft SQL Server (Local, Windows)


# Project Structure

airflow/
├── dags/
│   ├── earthquake_etl_pipeline_dag.py
│   └── src/
│       ├── __init__.py
│       ├── config.py
│       ├── data_extraction.py
│       ├── data_transformation.py
│       └── data_loading.py
│
├── data/
│   ├── raw/
│   │   └── api_response_YYYY-MM-DD_to_YYYY-MM-DD.json
│   └── processed/
│       └── api_response_YYYY-MM-DD_to_YYYY-MM-DD.csv
│
├── docker-compose.yml
├── Dockerfile
└── README.md


# ETL Pipeline Breakdown

1. Extract (API → Raw JSON)

  *  Fetches earthquake data from the USGS Earthquake API

  *  Uses HTTP GET requests with date-based batching

  *  Saves responses as JSON files in data/raw/


2. Transform (Raw JSON → Clean CSV)

  *  Reads JSON files from the raw directory

  *  Normalizes JSON nested structures (e.g. properties, geometry)

  *  Cleans and validates fields

  *  Writes cleaned data to data/processed/ as CSV files

3. Load (CSV → SQL Server)

  * Reads processed CSV files

  * Loads data into Microsoft SQL Server using SQLAlchemy

  * Uses SQL Authentication (required for Docker → Windows)


# Airflow Orchestration

  * DAG is scheduled (batch-based)

  * Tasks run in strict order:
   
    extract → transform → load


# Configuration

## SQL Server Connection

  * SQL Server runs locally on Windows

  * Airflow runs inside Docker as container

  * Connection uses:
    host: host.docker.internal
    port: 1433
    authentication: SQL Server Authentication


# Author

Luthando Mthembu
Data Engineering / Analytics

# License
[License](LICENSE)