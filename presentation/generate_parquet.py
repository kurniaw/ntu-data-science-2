"""
Generate local Parquet files from raw Olist CSVs.

Run once (or whenever source data changes):
    python presentation/generate_parquet.py

Output: presentation/data/*.parquet
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

ROOT  = Path(__file__).parent.parent
DATA  = ROOT / "data"
SEEDS = ROOT / "dbt_project" / "seeds"
OUT   = Path(__file__).parent / "data"
OUT.mkdir(exist_ok=True)

# ── Load raw CSVs ──────────────────────────────────────────────────────────────
print("Loading raw CSVs…")

orders_raw    = pd.read_csv(DATA / "olist_orders_dataset.csv")
customers_raw = pd.read_csv(DATA / "olist_customers_dataset.csv")
items_raw     = pd.read_csv(DATA / "olist_order_items_dataset.csv")
payments_raw  = pd.read_csv(DATA / "olist_order_payments_dataset.csv")
products_raw  = pd.read_csv(DATA / "olist_products_dataset.csv")
sellers_raw   = pd.read_csv(DATA / "olist_sellers_dataset.csv")
geo_raw       = pd.read_csv(DATA / "olist_geolocation_dataset.csv")
reviews_raw   = pd.read_csv(DATA / "olist_order_reviews_dataset.csv")
cat_trans     = pd.read_csv(SEEDS / "product_category_name_translation.csv")

# ── Type casts ─────────────────────────────────────────────────────────────────
_TS = [
    "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
for col in _TS:
    orders_raw[col] = pd.to_datetime(orders_raw[col], errors="coerce")

items_raw["order_item_id"] = pd.to_numeric(items_raw["order_item_id"], errors="coerce").astype("Int64")
items_raw["price"]         = pd.to_numeric(items_raw["price"],         errors="coerce")
items_raw["freight_value"] = pd.to_numeric(items_raw["freight_value"], errors="coerce")

customers_raw["customer_zip_code_prefix"] = (
    pd.to_numeric(customers_raw["customer_zip_code_prefix"], errors="coerce").astype("Int64")
)
sellers_raw["seller_zip_code_prefix"] = (
    pd.to_numeric(sellers_raw["seller_zip_code_prefix"], errors="coerce").astype("Int64")
)
payments_raw["payment_value"] = pd.to_numeric(payments_raw["payment_value"], errors="coerce")
reviews_raw["review_score"]   = pd.to_numeric(reviews_raw["review_score"],   errors="coerce").astype("Int64")

# ── dim_customers ──────────────────────────────────────────────────────────────
print("Building dim_customers…")

stg_customers = customers_raw.rename(columns={"customer_zip_code_prefix": "customer_zip_code"})

dim_customers = (
    stg_customers.groupby("customer_unique_id", sort=False)
    .agg(
        customer_id=("customer_id", "first"),
        customer_zip_code=("customer_zip_code", "first"),
        customer_city=("customer_city", "first"),
        customer_state=("customer_state", "first"),
    )
    .reset_index()
)

# ── dim_geography ──────────────────────────────────────────────────────────────
print("Building dim_geography…")

def _region(state: str) -> str:
    if state in {"AC", "AM", "AP", "PA", "RO", "RR"}:          return "North"
    if state in {"AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"}: return "Northeast"
    if state in {"DF", "GO", "MS", "MT"}:                       return "West"
    if state in {"ES", "MG", "RJ", "SP"}:                       return "Southeast"
    if state in {"PR", "RS", "SC"}:                             return "South"
    return "Unknown"

dim_geography = (
    geo_raw.dropna(subset=["geolocation_zip_code_prefix"])
    .groupby("geolocation_zip_code_prefix", sort=False)
    .agg(
        latitude=("geolocation_lat",  "mean"),
        longitude=("geolocation_lng", "mean"),
        city=("geolocation_city",  "first"),
        state=("geolocation_state", "first"),
    )
    .reset_index()
    .rename(columns={"geolocation_zip_code_prefix": "zip_code_prefix"})
)
dim_geography["zip_code_prefix"] = dim_geography["zip_code_prefix"].astype("Int64")
dim_geography["region"] = dim_geography["state"].map(_region)

zip_region = dim_geography[["zip_code_prefix", "region"]].drop_duplicates("zip_code_prefix")

# ── fct_orders (enriched: state, zip, region) ──────────────────────────────────
print("Building fct_orders…")

cid_map = stg_customers[["customer_id", "customer_unique_id", "customer_zip_code", "customer_state"]].drop_duplicates("customer_id")

fct_orders = (
    orders_raw.merge(cid_map, on="customer_id", how="left")
    [[
        "order_id", "customer_unique_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "customer_state", "customer_zip_code",
    ]]
)
fct_orders = fct_orders.merge(zip_region, left_on="customer_zip_code", right_on="zip_code_prefix", how="left")

# ── dim_product_category ───────────────────────────────────────────────────────
print("Building dim_product_category…")

def _category_type(cat_en: str | float) -> str:
    if not isinstance(cat_en, str):
        return "Other"
    ELECTRONICS = {
        "electronics", "computers", "computers_accessories", "telephony",
        "fixed_telephony", "tablets_printing_image", "audio", "cine_photo",
        "air_conditioning", "home_appliances", "home_appliances_2",
        "small_appliances", "small_appliances_home_oven_and_coffee",
    }
    FASHION = {
        "perfumery", "watches_gifts", "luggage_accessories",
        "fashion_bags_accessories", "fashion_shoes", "fashion_male_clothing",
        "fashio_female_clothing", "fashion_underwear_beach", "fashion_sport",
        "fashion_childrens_clothes",
    }
    HOME = {
        "bed_bath_table", "housewares", "furniture_decor",
        "furniture_living_room", "furniture_bedroom",
        "furniture_mattress_and_upholstery", "office_furniture",
        "kitchen_dining_laundry_garden_furniture", "home_confort",
        "home_comfort_2", "home_construction", "garden_tools",
        "costruction_tools_garden", "construction_tools_construction",
        "costruction_tools_tools", "construction_tools_lights",
        "construction_tools_safety", "christmas_supplies", "flowers",
    }
    HEALTH = {"health_beauty", "sports_leisure", "diapers_and_hygiene"}
    ART    = {"art", "arts_and_craftmanship", "music", "musical_instruments", "cds_dvds_musicals", "dvds_blu_ray"}
    BOOKS  = {"books_technical", "books_general_interest", "books_imported", "stationery"}
    TOYS   = {"toys", "baby", "consoles_games", "party_supplies"}
    AUTO   = {"auto"}
    FOOD   = {"food_drink", "food", "drinks", "la_cuisine"}
    if cat_en in ELECTRONICS: return "Electronics"
    if cat_en in FASHION:     return "Fashion"
    if cat_en in HOME:        return "Home"
    if cat_en in HEALTH:      return "Health & Beauty"
    if cat_en in ART:         return "Art & Culture"
    if cat_en in BOOKS:       return "Books & Media"
    if cat_en in TOYS:        return "Toys & Games"
    if cat_en in AUTO:        return "Automotive"
    if cat_en in FOOD:        return "Food & Drink"
    return "Other"

cat_src = (
    products_raw[["product_category_name"]].dropna()
    .drop_duplicates()
    .merge(cat_trans[["category_name", "category_name_english"]],
           left_on="product_category_name", right_on="category_name", how="left")
    .drop(columns=["category_name"])          # redundant with product_category_name
    .drop_duplicates(subset=["product_category_name"])
    .reset_index(drop=True)
)
cat_src["category_type"]   = cat_src["category_name_english"].map(_category_type)
cat_src["is_current"]      = True
cat_src["valid_from_date"] = pd.Timestamp.now()
cat_src["valid_to_date"]   = pd.NaT
cat_src["category_key"]    = cat_src.index + 1

dim_product_category = cat_src.rename(columns={"product_category_name": "category_name"})[
    ["category_key", "category_name", "category_name_english",
     "category_type", "valid_from_date", "valid_to_date", "is_current"]
]

# ── dim_products ───────────────────────────────────────────────────────────────
print("Building dim_products…")

dim_products = products_raw.copy()
# Handle the CSV misspellings
for src, dst in [
    ("product_name_lenght",        "product_name_length"),
    ("product_description_lenght", "product_description_length"),
]:
    if src in dim_products.columns:
        dim_products = dim_products.rename(columns={src: dst})

# ── dim_sellers ────────────────────────────────────────────────────────────────
print("Building dim_sellers…")

dim_sellers = sellers_raw.rename(columns={"seller_zip_code_prefix": "seller_zip_code"})

# ── fct_order_items (enriched: order timestamp + category) ────────────────────
print("Building fct_order_items…")

cat_lookup = (
    dim_products[["product_id", "product_category_name"]]
    .merge(
        dim_product_category[["category_name", "category_name_english", "category_type"]],
        left_on="product_category_name", right_on="category_name", how="left",
    )
)

fct_order_items = (
    items_raw
    .merge(fct_orders[["order_id", "order_purchase_timestamp"]], on="order_id", how="left")
    .merge(cat_lookup[["product_id", "product_category_name", "category_name_english", "category_type"]],
           on="product_id", how="left")
)

# ── fct_product_reviews ────────────────────────────────────────────────────────
print("Building fct_product_reviews…")

first_items = (
    items_raw[items_raw["order_item_id"] == 1][["order_id", "product_id"]]
    .drop_duplicates("order_id")
)

def _sentiment(score) -> str | None:
    if pd.isna(score): return None
    s = int(score)
    if s >= 4: return "positive"
    if s == 3: return "neutral"
    return "negative"

reviews_base = (
    reviews_raw[reviews_raw["review_id"].notna()]
    .drop_duplicates(subset=["review_id"])
    .merge(orders_raw[["order_id", "customer_id"]], on="order_id", how="inner")
    .merge(first_items, on="order_id", how="left")
    .merge(fct_orders[["order_id", "customer_unique_id"]], on="order_id", how="left")
)

reviews_base["review_sentiment"] = reviews_base["review_score"].map(_sentiment)
reviews_base["has_comment"]      = reviews_base["review_comment_message"].notna()
reviews_base["review_key"]       = reviews_base["review_id"].map(
    lambda rid: hashlib.md5(str(rid).encode()).hexdigest()
)

fct_product_reviews = reviews_base[[
    "review_key", "review_id", "order_id", "product_id", "customer_unique_id",
    "review_score", "review_sentiment", "has_comment",
]].rename(columns={
    "order_id":           "order_key",
    "product_id":         "product_key",
    "customer_unique_id": "customer_key",
})

# ── order_payments ─────────────────────────────────────────────────────────────
print("Building order_payments…")

order_payments = payments_raw[["order_id", "payment_type", "payment_value"]].copy()

# ── Save ───────────────────────────────────────────────────────────────────────
print("\nSaving parquet files…")

def _save(df: pd.DataFrame, name: str) -> None:
    path = OUT / f"{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"  ✓ {name}.parquet  ({len(df):,} rows)")

_save(fct_orders,           "fct_orders")
_save(fct_order_items,      "fct_order_items")
_save(dim_customers,        "dim_customers")
_save(dim_products,         "dim_products")
_save(dim_sellers,          "dim_sellers")
_save(dim_geography,        "dim_geography")
_save(dim_product_category, "dim_product_category")
_save(fct_product_reviews,  "fct_product_reviews")
_save(order_payments,       "order_payments")

print("\nDone. Run:  streamlit run presentation/dashboard.py")
