#!/usr/bin/env bash
# Full ELT pipeline: validate → ingest → transform → test
# Exits immediately on any failure (set -e).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "==> [1/4] Great Expectations: validating source CSVs..."
python great_expectations/validate.py

#echo "==> [2/4] Meltano: extracting and loading to BigQuery..."
cd elt
meltano elt tap-csv target-bigquery
cd "$SCRIPT_DIR"

echo "==> [3/4] dbt: building models..."
cd dbt_project
dbt run
dbt snapshot
cd "$SCRIPT_DIR"

echo "==> [4/4] dbt: running data quality tests..."
cd dbt_project
dbt test
cd "$SCRIPT_DIR"

echo ""
echo "Pipeline complete."
