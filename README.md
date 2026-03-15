# Production Data Warehouse for Olist E-commerce

This project repository contains the setup for a production data warehouse using BigQuery. The architecture involves an ELT (Extract, Load, Transform) pipeline orchestrated via **Meltano**, transformations performed with **dbt**, and data validation using **Great Expectations**.

![Brazilian E-Commerce Data Schema](assets/brazilian_ecommerce_data_schema.png)
Source: [https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

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
Modify the following files to replace the placeholders with your own values:
- `elt/meltano.yml`
- `dbt_project/profiles.yml`

## 2. Data Ingestion (ELT)
We use `meltano` to move our raw CSV data into BigQuery.
The current source data resides in `data/*.csv`.

**Setup Meltano**:
```bash
meltano install --plugin-type loader target-bigquery
```

**Run Pipeline**:
This will extract data using `tap-csv` and load it into your BigQuery project via `target-bigquery`.
```bash
cd elt
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
dbt deps                  # install packages (e.g. dbt_utils)
dbt run                   # build all layers: staging → core → analytics
dbt snapshot              # capture Type-2 SCD snapshots
dbt test                  # run data quality tests
```

> Run `dbt run` before `dbt run --select "analytics.*"` — the analytics layer depends on core models (`fct_orders`, `dim_customers`, `dim_products`) that must be materialised first.

## 4. Data Validation
To ensure incoming `data/*.csv` constraints (like non-negative prices, correct date formats) before hitting downstream dependencies, we employ Great Expectations.

```bash
great_expectations checkpoint run olist_raw_checkpoint
```
