"""
Validate all raw Olist CSVs using Great Expectations (ephemeral context).

Builds the datasource, suites, and checkpoint entirely in memory on each run —
no serialisation to disk, no stale YAML to manage.

Usage:
    python great_expectations/validate.py

Exit codes:
    0  all expectations passed
    1  one or more expectations failed
"""

import sys
import os
import great_expectations as gx

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# ---------------------------------------------------------------------------
# Context (in-memory, no files written)
# ---------------------------------------------------------------------------
context = gx.get_context(mode="ephemeral")

# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------
data_source = context.data_sources.add_pandas_filesystem(
    name="olist_csv_datasource",
    base_directory=DATA_DIR,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
E = gx.expectations


def not_null(col):
    return E.ExpectColumnValuesToNotBeNull(column=col)


def unique(col):
    return E.ExpectColumnValuesToBeUnique(column=col)


def between(col, min_value=None, max_value=None):
    return E.ExpectColumnValuesToBeBetween(
        column=col, min_value=min_value, max_value=max_value
    )


def in_set(col, values):
    return E.ExpectColumnValuesToBeInSet(column=col, value_set=values)


# ---------------------------------------------------------------------------
# Datasets: (asset_name, csv_filename, suite_name, [expectations])
# ---------------------------------------------------------------------------
DATASETS = [
    (
        "order_items",
        "olist_order_items_dataset.csv",
        "olist_order_items_suite",
        [
            not_null("order_id"),
            not_null("order_item_id"),
            not_null("product_id"),
            not_null("seller_id"),
            not_null("price"),
            not_null("freight_value"),
            between("order_item_id", min_value=1),
            between("price",         min_value=0),
            between("freight_value", min_value=0),
        ],
    ),
    (
        "order_payments",
        "olist_order_payments_dataset.csv",
        "olist_order_payments_suite",
        [
            not_null("order_id"),
            not_null("payment_type"),
            not_null("payment_value"),
            in_set("payment_type", ["credit_card", "debit_card", "boleto", "voucher", "not_defined"]),
            between("payment_installments", min_value=0),  # 0 = pay in full (data quirk)
            between("payment_value",        min_value=0),
        ],
    ),
    (
        "orders",
        "olist_orders_dataset.csv",
        "olist_orders_suite",
        [
            not_null("order_id"),
            not_null("customer_id"),
            not_null("order_status"),
            not_null("order_purchase_timestamp"),
            unique("order_id"),
            in_set("order_status", [
                "delivered", "shipped", "canceled", "invoiced",
                "processing", "unavailable", "approved", "created",
            ]),
        ],
    ),
    (
        "order_reviews",
        "olist_order_reviews_dataset.csv",
        "olist_order_reviews_suite",
        [
            not_null("review_id"),
            not_null("order_id"),
            not_null("review_score"),
            between("review_score", min_value=1, max_value=5),
        ],
    ),
    (
        "customers",
        "olist_customers_dataset.csv",
        "olist_customers_suite",
        [
            not_null("customer_id"),
            not_null("customer_unique_id"),
        ],
    ),
    (
        "products",
        "olist_products_dataset.csv",
        "olist_products_suite",
        [
            not_null("product_id"),
            not_null("product_category_name"),
        ],
    ),
    (
        "sellers",
        "olist_sellers_dataset.csv",
        "olist_sellers_suite",
        [
            not_null("seller_id"),
        ],
    ),
]

# ---------------------------------------------------------------------------
# Build validation definitions
# ---------------------------------------------------------------------------
validation_definitions = []

for asset_name, csv_file, suite_name, expectations in DATASETS:
    asset     = data_source.add_csv_asset(name=asset_name)
    batch_def = asset.add_batch_definition_path(name=f"{asset_name}_batch", path=csv_file)
    suite     = context.suites.add(gx.ExpectationSuite(name=suite_name, expectations=expectations))
    val_def   = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"{asset_name}_validation", data=batch_def, suite=suite)
    )
    validation_definitions.append(val_def)

# ---------------------------------------------------------------------------
# Checkpoint + run
# ---------------------------------------------------------------------------
checkpoint = context.checkpoints.add(
    gx.Checkpoint(name="olist_raw_checkpoint", validation_definitions=validation_definitions)
)

print("Running olist_raw_checkpoint...\n")
result = checkpoint.run()

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
all_passed = True
for suite_name, val_result in result.run_results.items():
    success     = val_result.success
    n_pass      = val_result.statistics["successful_expectations"]
    n_total     = val_result.statistics["evaluated_expectations"]
    status      = "PASS" if success else "FAIL"
    print(f"  [{status}] {suite_name}  ({n_pass}/{n_total} expectations)")
    if not success:
        all_passed = False

print()
if all_passed:
    print("All expectations passed.")
    sys.exit(0)
else:
    print("Validation FAILED. Fix source data before loading to BigQuery.")
    sys.exit(1)
