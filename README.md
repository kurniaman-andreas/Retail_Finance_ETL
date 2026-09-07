# Retail Financing ETL

Core local ETL pipeline for the provided Excel workbook:

`Excel workbook -> Bronze PostgreSQL tables -> Staging PostgreSQL tables -> Gold star schema`

## Overview

A concise description of the Retail Financing ETL study case, its objectives, and the data domain. It demonstrates how to transform a static Excel workbook into a robust star‑schema data warehouse.

## Business Problem

Retail finance organizations often face fragmented source systems, inconsistent data types, and missing quality checks, making it hard to produce reliable analytics on customers, branches, and transactions.

## Solution Architecture

The pipeline orchestrates the flow:

Excel workbook → Bronze tables → Staging tables → **Data‑Quality Validation** (`dq_validation`) → Gold star schema. Airflow schedules each step, SQL scripts are externalized, and a dedicated DQ task ensures only clean data reaches the DWH.

## Technology Stack

- **Airflow** – orchestration and scheduling
- **PostgreSQL** – data warehouse
- **Python** – DAG definition and helper utilities
- **Docker Compose** – local development environment
- **SQL** – modular scripts for each stage

## Data Model

- **Bronze** preserves the three source worksheets plus batch and source metadata.
- **Staging** standardises types and values. It retains the latest `updated_at` per `transaction_id` and the final worksheet row per `customer_id`.
- **Gold** (final analytical layer) contains the tables `transactions`, `customers`, and `branches`, each with a primary key (`transaction_id`, `customer_id`, `branch_id`) and one row per entity. No separate `dim_` or `fact_` prefixes are used.

## Project Layout

```
.
├── dags
│   └── retail_financing_pipeline.py   # Airflow DAG (now uses external .sql files)
├── sql
│   ├── staging
│   │   ├── stg_transactions.sql
│   │   ├── stg_customers.sql
│   │   └── stg_branches.sql
│   ├── dwh
│   │   ├── load_customers.sql
│   │   ├── load_branches.sql
│   │   └── load_transactions.sql
│   └── dq
│       └── validate.sql            # Data‑Quality validation (runs after staging)
└── README.md                         # This file
```

The DAG now loads each of the above SQL files using the helper `execute_sql_file(connection, relative_path, params)`. The helper reads the SQL from `SQL_DIR` (default `/opt/airflow/sql`, configurable via the `SQL_DIR` environment variable) and executes it with the provided parameters (e.g. `%(batch_date)s`).

## New Data‑Quality Validation Step

A **DQ validation** task (`dq_validation`) has been added between the staging and DWH load stages. It runs `sql/dq/validate.sql`, which aggregates several rule checks (null checks, duplicate detection, etc.). If any rule returns a non‑zero `failed_count`, the task fails and downstream loading is aborted.

## Run Locally

1. (Optional) Create the runtime configuration with `Copy-Item .env.example .env` to override the supplied local defaults.
2. Build and start PostgreSQL and Airflow:
   ```powershell
   docker compose up --build -d
   ```
3. Open `http://localhost:8080` and sign in with the credentials in `.env`.
4. Trigger `retail_financing_pipeline` from the Airflow UI.

The DAG is manual (`schedule=None`) because the source is a single static workbook snapshot. Each DAG run uses its logical date as `batch_id`. Retries replace data for that batch in Bronze and Staging; Gold is rebuilt from the current batch.

To remove local containers and database data after testing, run:

```powershell
docker compose down -v
```
