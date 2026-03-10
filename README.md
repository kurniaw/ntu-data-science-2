# Production Data Warehouse for Olist E-commerce

This project repository contains the setup for a production data warehouse using BigQuery. The architecture involves an ELT (Extract, Load, Transform) pipeline orchestrated via **Meltano**, transformations performed with **dbt**, and data validation using **Great Expectations**.

## Prerequisites
- **Conda**: Ensure you have Miniconda or Anaconda installed.
- **Google Cloud Platform**: A valid GCP Project with BigQuery enabled, and a Service Account with `BigQuery Data Editor` and `BigQuery Job User` roles.

## 1. Environment Setup
To set up the project dependencies securely without affecting system packages, we use Conda.
```bash
conda env create -f environment.yml
conda activate ntu-project-2
```

Create new .env file and replace the placeholders with your own values.
```
GCP_PROJECT_ID=<gcp project id>
GCP_CREDENTIALS_PATH=<absolute path to json file>
```

## 2. Data Ingestion (ELT)
We use `meltano` to move our raw CSV data into BigQuery.
The current source data resides in `data/*.csv`.

**Setup Meltano**:
```bash
meltano install
```

**Run Pipeline**:
This will extract data using `tap-csv` and load it into your BigQuery project via `target-bigquery`.
```bash
meltano elt tap-csv target-bigquery
```

## 3. Data Warehouse Design
The data is transformed according to a Star Schema directly within BigQuery using `dbt`.

- **Staging**: Light data cleaning (types, basic standardizations) under `models/staging`.
- **Core Models**:
  - `dim_customers`, `dim_products`, `dim_sellers`
  - `fct_orders`, `fct_order_items`
- **Snapshots**: Type-2 Slowly Changing Dimensions (SCD) for order status tracking, saved in `snapshots/`.

**Run dbt Models**:
```bash
cd dbt_project
dbt parse
dbt run
dbt test
dbt snapshot
```

## 4. Data Validation
To ensure incoming `data/*.csv` constraints (like non-negative prices, correct date formats) before hitting downstream dependencies, we employ Great Expectations.

```bash
great_expectations checkpoint run olist_raw_checkpoint
```
