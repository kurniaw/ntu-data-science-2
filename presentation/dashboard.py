"""
Olist E-Commerce — Executive Analytics Dashboard
=================================================
Connects directly to the BigQuery analytics layer built by dbt.

Dataset layout (dbt default generate_schema_name):
  - stg_olist            → core layer: fct_orders, fct_order_items, dim_customers,
                           dim_products, dim_sellers
  - stg_olist_analytics  → analytics layer: dim_date, dim_geography, dim_payment_type,
                           dim_product_category, fct_product_reviews
  - raw_olist            → raw loaded CSVs (order_payments used directly)

Override via env vars if your dbt setup uses different dataset names:
  BQ_CORE_DATASET      (default: stg_olist)
  BQ_ANALYTICS_DATASET (default: stg_olist_analytics)

Run:
  streamlit run presentation/dashboard.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# ── Load .env from project root ───────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent / ".env")

# ── BigQuery config ───────────────────────────────────────────────────────────
PROJECT   = os.getenv("GCP_PROJECT_ID", "ntu-data-science-ai")
CORE      = os.getenv("BQ_CORE_DATASET",      "stg_olist")
ANALYTICS = os.getenv("BQ_ANALYTICS_DATASET", "stg_olist_analytics")
RAW       = "raw_olist"

def _tbl(dataset: str, table: str) -> str:
    return f"`{PROJECT}.{dataset}.{table}`"

# ── Design tokens ─────────────────────────────────────────────────────────────
PAL = {
    "blue":   "#3b82f6",
    "green":  "#22c55e",
    "amber":  "#f59e0b",
    "red":    "#ef4444",
    "teal":   "#14b8a6",
    "purple": "#a855f7",
    "pink":   "#ec4899",
    "indigo": "#6366f1",
}
SEQ = list(PAL.values())
BG   = "rgba(0,0,0,0)"
GRID = "rgba(255,255,255,0.06)"
FONT = "#e2e8f0"
MUTED = "#94a3b8"


def _layout(title: str = "", height: int = 380) -> dict:
    return dict(
        title=dict(text=title, font=dict(color=FONT, size=13)),
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        font=dict(color=FONT, family="system-ui, -apple-system, sans-serif", size=12),
        xaxis=dict(gridcolor=GRID, zeroline=False, showline=False, tickfont=dict(color=MUTED)),
        yaxis=dict(gridcolor=GRID, zeroline=False, showline=False, tickfont=dict(color=MUTED)),
        height=height,
        margin=dict(l=0, r=10, t=40, b=0),
        legend=dict(bgcolor=BG, font=dict(size=11, color=FONT)),
        hoverlabel=dict(bgcolor="#1e293b", font_color=FONT),
    )


# ── BigQuery client (cached singleton) ───────────────────────────────────────
@st.cache_resource
def _client() -> bigquery.Client:
    creds_path = os.getenv("GCP_CREDENTIALS_PATH", "")
    fallback = str(
        Path(__file__).parent.parent
        / "credentials"
        / "ntu-data-science-ai-4d35c69fac50.json"
    )
    path = creds_path if (creds_path and Path(creds_path).exists()) else fallback
    creds = service_account.Credentials.from_service_account_file(
        path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(project=PROJECT, credentials=creds)


def _bq(sql: str) -> pd.DataFrame:
    return _client().query(sql).to_dataframe()


# ── Query helpers (all cached 1 h) ────────────────────────────────────────────
@st.cache_data(ttl=3600)
def q_overview_kpis() -> pd.DataFrame:
    return _bq(f"""
    WITH item_rev AS (
        SELECT order_id, SUM(price + freight_value) AS order_value
        FROM {_tbl(CORE, 'fct_order_items')}
        GROUP BY order_id
    ),
    delivery AS (
        SELECT
            COUNT(*)                                                        AS delivered,
            AVG(DATE_DIFF(
                DATE(order_delivered_customer_date),
                DATE(order_purchase_timestamp), DAY))                       AS avg_days,
            SAFE_DIVIDE(
                COUNTIF(order_delivered_customer_date
                        <= order_estimated_delivery_date),
                COUNT(*))                                                   AS on_time_rate
        FROM {_tbl(CORE, 'fct_orders')}
        WHERE order_status = 'delivered'
          AND order_delivered_customer_date IS NOT NULL
          AND order_purchase_timestamp      IS NOT NULL
    )
    SELECT
        SUM(ir.order_value)            AS gmv,
        COUNT(DISTINCT ir.order_id)    AS total_orders,
        AVG(ir.order_value)            AS aov,
        ANY_VALUE(d.delivered)         AS delivered,
        ANY_VALUE(d.avg_days)          AS avg_days,
        ANY_VALUE(d.on_time_rate)      AS on_time_rate
    FROM item_rev ir
    CROSS JOIN delivery d
    """)


@st.cache_data(ttl=3600)
def q_review_summary() -> pd.DataFrame:
    return _bq(f"""
    SELECT
        AVG(review_score)  AS avg_score,
        COUNT(*)           AS total_reviews,
        COUNTIF(review_sentiment = 'positive') AS positive_count,
        COUNTIF(review_sentiment = 'negative') AS negative_count
    FROM {_tbl(ANALYTICS, 'fct_product_reviews')}
    WHERE review_score IS NOT NULL
    """)


@st.cache_data(ttl=3600)
def q_monthly_revenue() -> pd.DataFrame:
    return _bq(f"""
    SELECT
        FORMAT_DATE('%Y-%m', DATE(o.order_purchase_timestamp)) AS month,
        SUM(oi.price + oi.freight_value)                       AS revenue,
        COUNT(DISTINCT o.order_id)                             AS orders
    FROM {_tbl(CORE, 'fct_orders')} o
    JOIN {_tbl(CORE, 'fct_order_items')} oi ON o.order_id = oi.order_id
    WHERE o.order_purchase_timestamp IS NOT NULL
      AND o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY 1
    ORDER BY 1
    """)


@st.cache_data(ttl=3600)
def q_order_status() -> pd.DataFrame:
    return _bq(f"""
    SELECT order_status, COUNT(*) AS cnt
    FROM {_tbl(CORE, 'fct_orders')}
    GROUP BY 1
    ORDER BY cnt DESC
    """)


@st.cache_data(ttl=3600)
def q_category_revenue() -> pd.DataFrame:
    return _bq(f"""
    SELECT
        COALESCE(pc.category_name_english, dp.product_category_name, 'Unknown') AS category,
        COALESCE(pc.category_type, 'Other')                                      AS category_type,
        SUM(oi.price)                                                            AS revenue,
        COUNT(DISTINCT oi.order_id)                                              AS orders
    FROM {_tbl(CORE, 'fct_order_items')} oi
    JOIN {_tbl(CORE, 'dim_products')} dp
        ON oi.product_id = dp.product_id
    LEFT JOIN {_tbl(ANALYTICS, 'dim_product_category')} pc
        ON dp.product_category_name = pc.category_name
        AND pc.is_current = TRUE
    GROUP BY 1, 2
    ORDER BY revenue DESC
    LIMIT 15
    """)


@st.cache_data(ttl=3600)
def q_payment_mix() -> pd.DataFrame:
    return _bq(f"""
    SELECT
        payment_type,
        COUNT(DISTINCT order_id)                      AS orders,
        SUM(SAFE_CAST(payment_value AS FLOAT64))      AS total_value
    FROM {_tbl(RAW, 'order_payments')}
    GROUP BY 1
    ORDER BY orders DESC
    """)


@st.cache_data(ttl=3600)
def q_rfm() -> pd.DataFrame:
    return _bq(f"""
    WITH max_dt AS (
        SELECT MAX(DATE(order_purchase_timestamp)) AS max_date
        FROM {_tbl(CORE, 'fct_orders')}
    ),
    rfm AS (
        SELECT
            o.customer_unique_id,
            DATE_DIFF(md.max_date,
                MAX(DATE(o.order_purchase_timestamp)), DAY)  AS recency,
            COUNT(DISTINCT o.order_id)                       AS frequency,
            SUM(oi.price)                                    AS monetary
        FROM {_tbl(CORE, 'fct_orders')} o
        JOIN {_tbl(CORE, 'fct_order_items')} oi ON o.order_id = oi.order_id
        CROSS JOIN max_dt md
        WHERE o.order_status IN ('delivered', 'shipped', 'invoiced')
        GROUP BY 1, md.max_date
    )
    SELECT
        CASE
            WHEN recency <= 90  AND frequency >= 3 AND monetary >= 300 THEN 'Champions'
            WHEN recency <= 180 AND frequency >= 2                      THEN 'Loyal'
            WHEN recency <= 90                                          THEN 'Promising'
            WHEN recency <= 365                                         THEN 'At Risk'
            ELSE 'Inactive'
        END AS segment,
        COUNT(*) AS customers
    FROM rfm
    GROUP BY 1
    ORDER BY customers DESC
    """)


@st.cache_data(ttl=3600)
def q_customer_state() -> pd.DataFrame:
    return _bq(f"""
    SELECT customer_state, COUNT(DISTINCT customer_unique_id) AS customers
    FROM {_tbl(CORE, 'dim_customers')}
    GROUP BY 1
    ORDER BY customers DESC
    LIMIT 15
    """)


@st.cache_data(ttl=3600)
def q_order_frequency() -> pd.DataFrame:
    return _bq(f"""
    SELECT
        CASE
            WHEN cnt = 1  THEN '1 order'
            WHEN cnt = 2  THEN '2 orders'
            WHEN cnt = 3  THEN '3 orders'
            WHEN cnt >= 4 THEN '4+ orders'
        END AS bucket,
        COUNT(*) AS customers
    FROM (
        SELECT customer_unique_id, COUNT(DISTINCT order_id) AS cnt
        FROM {_tbl(CORE, 'fct_orders')}
        GROUP BY 1
    )
    GROUP BY 1
    ORDER BY bucket
    """)


@st.cache_data(ttl=3600)
def q_delivery_by_state() -> pd.DataFrame:
    return _bq(f"""
    SELECT
        c.customer_state                                          AS state,
        AVG(DATE_DIFF(
            DATE(o.order_delivered_customer_date),
            DATE(o.order_purchase_timestamp), DAY))               AS avg_days,
        SAFE_DIVIDE(
            COUNTIF(o.order_delivered_customer_date
                    <= o.order_estimated_delivery_date),
            COUNT(*))                                             AS on_time_rate,
        COUNT(*)                                                  AS orders
    FROM {_tbl(CORE, 'fct_orders')} o
    JOIN {_tbl(CORE, 'dim_customers')} c
        ON o.customer_unique_id = c.customer_unique_id
    WHERE o.order_status = 'delivered'
      AND o.order_delivered_customer_date IS NOT NULL
      AND o.order_purchase_timestamp      IS NOT NULL
    GROUP BY 1
    HAVING orders >= 50
    ORDER BY avg_days
    """)


@st.cache_data(ttl=3600)
def q_monthly_ontime() -> pd.DataFrame:
    return _bq(f"""
    SELECT
        FORMAT_DATE('%Y-%m', DATE(order_purchase_timestamp)) AS month,
        SAFE_DIVIDE(
            COUNTIF(order_delivered_customer_date
                    <= order_estimated_delivery_date),
            COUNT(*))                                         AS on_time_rate
    FROM {_tbl(CORE, 'fct_orders')}
    WHERE order_status = 'delivered'
      AND order_delivered_customer_date IS NOT NULL
      AND order_purchase_timestamp      IS NOT NULL
    GROUP BY 1
    ORDER BY 1
    """)


@st.cache_data(ttl=3600)
def q_review_distribution() -> pd.DataFrame:
    return _bq(f"""
    SELECT review_score, COUNT(*) AS cnt
    FROM {_tbl(ANALYTICS, 'fct_product_reviews')}
    WHERE review_score IS NOT NULL
    GROUP BY 1
    ORDER BY 1
    """)


@st.cache_data(ttl=3600)
def q_sentiment_by_category() -> pd.DataFrame:
    return _bq(f"""
    SELECT
        COALESCE(pc.category_type, 'Other')                              AS category_type,
        SAFE_DIVIDE(COUNTIF(r.review_sentiment = 'positive'), COUNT(*)) AS positive_rate,
        AVG(r.review_score)                                              AS avg_score,
        COUNT(*)                                                         AS cnt
    FROM {_tbl(ANALYTICS, 'fct_product_reviews')} r
    JOIN {_tbl(CORE, 'dim_products')} dp
        ON r.product_key = dp.product_id
    LEFT JOIN {_tbl(ANALYTICS, 'dim_product_category')} pc
        ON dp.product_category_name = pc.category_name
        AND pc.is_current = TRUE
    WHERE r.review_score IS NOT NULL
    GROUP BY 1
    HAVING cnt >= 100
    ORDER BY positive_rate DESC
    """)


@st.cache_data(ttl=3600)
def q_monthly_rating() -> pd.DataFrame:
    return _bq(f"""
    SELECT
        FORMAT_DATE('%Y-%m', DATE(o.order_purchase_timestamp)) AS month,
        AVG(r.review_score)                                     AS avg_score,
        COUNT(*)                                                AS reviews
    FROM {_tbl(ANALYTICS, 'fct_product_reviews')} r
    JOIN {_tbl(CORE, 'fct_orders')} o
        ON r.order_key = o.order_id
    WHERE r.review_score IS NOT NULL
      AND o.order_purchase_timestamp IS NOT NULL
    GROUP BY 1
    ORDER BY 1
    """)


@st.cache_data(ttl=3600)
def q_total_products() -> int:
    df = _bq(f"SELECT COUNT(DISTINCT product_id) AS n FROM {_tbl(CORE, 'dim_products')}")
    return int(df.iloc[0]["n"])


# ── Geography & Logistics queries ─────────────────────────────────────────────

@st.cache_data(ttl=3600)
def q_customer_order_map() -> pd.DataFrame:
    return _bq(f"""
    WITH geo AS (
        SELECT zip_code_prefix,
               AVG(SAFE_CAST(latitude  AS FLOAT64)) AS lat,
               AVG(SAFE_CAST(longitude AS FLOAT64)) AS lng,
               ANY_VALUE(city)    AS city,
               ANY_VALUE(state)   AS state,
               ANY_VALUE(region)  AS region
        FROM {_tbl(ANALYTICS, 'dim_geography')}
        GROUP BY 1
    )
    SELECT
        g.lat, g.lng, g.city, g.state, g.region,
        COUNT(DISTINCT o.order_id)  AS orders,
        SUM(oi.price)               AS revenue
    FROM {_tbl(CORE, 'fct_orders')} o
    JOIN {_tbl(CORE, 'dim_customers')}   c  ON o.customer_unique_id = c.customer_unique_id
    JOIN {_tbl(CORE, 'fct_order_items')} oi ON o.order_id           = oi.order_id
    JOIN geo g ON CAST(c.customer_zip_code AS INT64) = CAST(g.zip_code_prefix AS INT64)
    WHERE g.lat IS NOT NULL
    GROUP BY 1,2,3,4,5
    """)


@st.cache_data(ttl=3600)
def q_seller_map() -> pd.DataFrame:
    return _bq(f"""
    WITH geo AS (
        SELECT zip_code_prefix,
               AVG(SAFE_CAST(latitude  AS FLOAT64)) AS lat,
               AVG(SAFE_CAST(longitude AS FLOAT64)) AS lng,
               ANY_VALUE(city)   AS city,
               ANY_VALUE(state)  AS state,
               ANY_VALUE(region) AS region
        FROM {_tbl(ANALYTICS, 'dim_geography')}
        GROUP BY 1
    ),
    sellers AS (
        SELECT seller_zip_code, COUNT(DISTINCT seller_id) AS seller_count
        FROM {_tbl(CORE, 'dim_sellers')}
        GROUP BY 1
    )
    SELECT g.lat, g.lng, g.city, g.state, g.region, s.seller_count
    FROM sellers s
    JOIN geo g ON CAST(s.seller_zip_code AS INT64) = CAST(g.zip_code_prefix AS INT64)
    WHERE g.lat IS NOT NULL
    """)


@st.cache_data(ttl=3600)
def q_regional_performance() -> pd.DataFrame:
    return _bq(f"""
    WITH geo AS (
        SELECT zip_code_prefix, ANY_VALUE(region) AS region
        FROM {_tbl(ANALYTICS, 'dim_geography')}
        GROUP BY 1
    )
    SELECT
        g.region,
        COUNT(DISTINCT o.order_id)                                              AS orders,
        SUM(oi.price)                                                           AS revenue,
        AVG(DATE_DIFF(o.order_delivered_customer_date,
                      o.order_purchase_timestamp, DAY))                         AS avg_delivery_days
    FROM {_tbl(CORE, 'fct_orders')} o
    JOIN {_tbl(CORE, 'dim_customers')}   c  ON o.customer_unique_id = c.customer_unique_id
    JOIN {_tbl(CORE, 'fct_order_items')} oi ON o.order_id           = oi.order_id
    JOIN geo g ON CAST(c.customer_zip_code AS INT64) = CAST(g.zip_code_prefix AS INT64)
    WHERE o.order_delivered_customer_date IS NOT NULL
    GROUP BY 1
    ORDER BY revenue DESC
    """)


# ── Formatters ────────────────────────────────────────────────────────────────
def _brl(v: float) -> str:
    return f"R${v:,.0f}"

def _pct(v: float) -> str:
    return f"{v * 100:.1f}%"

def _num(v: float) -> str:
    return f"{int(v):,}"

def _fmt_cat(s: str) -> str:
    """'bed_bath_table' → 'Bed Bath Table'"""
    return s.replace("_", " ").title()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE SETUP
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Olist Analytics — Executive Dashboard",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
/* Tighten top padding */
.block-container { padding-top: 1.2rem !important; padding-bottom: 0 !important; }

/* Metric cards */
[data-testid="metric-container"] {
    background: #1e293b;
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 0.75rem;
    padding: 1rem 1.3rem !important;
}
[data-testid="stMetricValue"]  { font-size: 1.75rem !important; font-weight: 700 !important; }
[data-testid="stMetricLabel"]  { font-size: 0.76rem !important; color: #94a3b8 !important;
                                  text-transform: uppercase; letter-spacing: 0.05em; }
[data-testid="stMetricDelta"]  { font-size: 0.8rem !important; }

/* Tab labels */
[data-testid="stTabs"] button[role="tab"] {
    font-weight: 600;
    font-size: 0.88rem;
    letter-spacing: 0.02em;
}
[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
    color: #3b82f6 !important;
    border-bottom-color: #3b82f6 !important;
}

/* Horizontal rule */
hr { border-color: rgba(255,255,255,0.08) !important; margin: 0.6rem 0 !important; }

/* Sidebar */
[data-testid="stSidebar"] { background: #1e293b !important; border-right: 1px solid rgba(255,255,255,0.06); }
</style>
""", unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏭 Olist Analytics")
    st.markdown("**Executive Dashboard**")
    st.markdown("---")
    st.markdown("**Data Source**")
    st.markdown(f"Project: `{PROJECT}`")
    st.markdown(f"Core: `{CORE}`")
    st.markdown(f"Analytics: `{ANALYTICS}`")
    st.markdown("**Dataset Period**")
    st.markdown("Sep 2016 – Oct 2018")
    st.markdown("---")
    st.markdown("**Stack**")
    st.caption("Google BigQuery · dbt · Meltano · Great Expectations")
    st.markdown("---")
    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption("Cache TTL: 60 min")


# ── Page header ───────────────────────────────────────────────────────────────
st.markdown(
    "## Olist E-Commerce &nbsp;·&nbsp; Analytics Dashboard",
    unsafe_allow_html=True,
)
st.caption("Brazilian e-commerce platform · Sep 2016 – Oct 2018 · Data via BigQuery star schema")
st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
TAB_OVERVIEW, TAB_REVENUE, TAB_CUSTOMERS, TAB_OPS, TAB_PRODUCTS, TAB_GEO = st.tabs([
    "📊 Overview",
    "💰 Revenue & Growth",
    "👥 Customer Intelligence",
    "🚚 Operations & Delivery",
    "⭐ Products & Sentiment",
    "🗺️ Geography & Logistics",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 · OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
with TAB_OVERVIEW:
    with st.spinner("Loading overview data…"):
        kpis  = q_overview_kpis().iloc[0]
        score = q_review_summary().iloc[0]

    # ── KPI row ──────────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total GMV",          _brl(kpis["gmv"]))
    c2.metric("Total Orders",       _num(kpis["total_orders"]))
    c3.metric("Avg Order Value",    _brl(kpis["aov"]))
    c4.metric("On-Time Delivery",   _pct(kpis["on_time_rate"]))
    c5.metric("Avg Review Score",   f"{score['avg_score']:.2f} / 5 ★")

    st.markdown("---")

    # ── Charts row ───────────────────────────────────────────────────────────
    col_l, col_m, col_r = st.columns([3, 1.4, 1.4])

    with col_l:
        with st.spinner():
            monthly = q_monthly_revenue()
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=monthly["month"], y=monthly["revenue"],
            mode="lines+markers",
            name="GMV (R$)",
            line=dict(color=PAL["blue"], width=2.5),
            marker=dict(size=4),
            fill="tozeroy",
            fillcolor="rgba(59,130,246,0.10)",
        ))
        fig.update_layout(**_layout("Monthly GMV (R$) — Sep 2016 to Oct 2018", 320))
        fig.update_yaxes(tickprefix="R$", tickformat=",.0f")
        st.plotly_chart(fig, use_container_width=True)

    with col_m:
        with st.spinner():
            status_df = q_order_status()
        fig = px.pie(
            status_df, values="cnt", names="order_status",
            hole=0.55, color_discrete_sequence=SEQ,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label",
                          textfont_size=10)
        fig.update_layout(**_layout("Order Status", 320))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        with st.spinner():
            cat_df = q_category_revenue().head(6)
        cat_df["category"] = cat_df["category"].map(_fmt_cat)
        fig = px.bar(
            cat_df, x="revenue", y="category", orientation="h",
            color_discrete_sequence=[PAL["teal"]],
            labels={"revenue": "Revenue (R$)", "category": ""},
        )
        fig.update_layout(**_layout("Top 6 Categories by Revenue", 320))
        fig.update_xaxes(tickprefix="R$", tickformat=",.0f")
        st.plotly_chart(fig, use_container_width=True)

    # ── Secondary KPI row ────────────────────────────────────────────────────
    st.markdown("---")
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Avg Delivery Days",  f"{kpis['avg_days']:.1f} days")
    s2.metric("Delivered Orders",   _num(kpis["delivered"]))
    s3.metric("Total Reviews",      _num(score["total_reviews"]))
    s4.metric("Positive Reviews",   _pct(score["positive_count"] / score["total_reviews"]))


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 · REVENUE & GROWTH
# ══════════════════════════════════════════════════════════════════════════════
with TAB_REVENUE:
    with st.spinner("Loading revenue data…"):
        monthly   = q_monthly_revenue()
        cat_df    = q_category_revenue()
        pay_df    = q_payment_mix()
        kpis      = q_overview_kpis().iloc[0]

    cat_df["category"] = cat_df["category"].map(_fmt_cat)
    monthly["mom_growth"] = monthly["revenue"].pct_change() * 100
    peak = monthly.loc[monthly["revenue"].idxmax()]
    best = cat_df.iloc[0]

    # ── KPI row ──────────────────────────────────────────────────────────────
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Total GMV",         _brl(kpis["gmv"]))
    r2.metric("Peak Month Revenue", _brl(peak["revenue"]), peak["month"])
    r3.metric("MoM Growth (latest)", f"{monthly['mom_growth'].iloc[-1]:.1f}%")
    r4.metric("Top Category",       best["category"], _brl(best["revenue"]))

    st.markdown("---")

    # ── Dual-axis: revenue bars + orders line ─────────────────────────────
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=monthly["month"], y=monthly["revenue"],
        name="Revenue (R$)",
        marker_color=PAL["blue"],
        opacity=0.82,
        yaxis="y1",
    ))
    fig.add_trace(go.Scatter(
        x=monthly["month"], y=monthly["orders"],
        name="Orders",
        mode="lines+markers",
        line=dict(color=PAL["amber"], width=2.2),
        marker=dict(size=4),
        yaxis="y2",
    ))
    fig.update_layout(**_layout("Monthly Revenue (R$) & Order Volume", 360))
    fig.update_layout(
        yaxis=dict(title="Revenue (R$)", gridcolor=GRID,
                   tickprefix="R$", tickformat=",.0f", tickfont=dict(color=MUTED)),
        yaxis2=dict(title="Orders", overlaying="y", side="right",
                    gridcolor="rgba(0,0,0,0)", tickfont=dict(color=MUTED)),
        barmode="overlay",
        legend=dict(x=0.01, y=0.99, bgcolor=BG),
    )
    st.plotly_chart(fig, use_container_width=True)

    col_cat, col_pay = st.columns([3, 2])

    with col_cat:
        fig = px.bar(
            cat_df.head(12), x="revenue", y="category", orientation="h",
            color="category_type", color_discrete_sequence=SEQ,
            labels={"revenue": "Revenue (R$)", "category": ""},
        )
        fig.update_layout(**_layout("Top 12 Categories by Revenue", 420))
        fig.update_xaxes(tickprefix="R$", tickformat=",.0f")
        st.plotly_chart(fig, use_container_width=True)

    with col_pay:
        LABELS = {
            "credit_card":  "Credit Card",
            "boleto":        "Boleto",
            "voucher":       "Voucher",
            "debit_card":    "Debit Card",
            "not_defined":   "Other",
        }
        pay_df["label"] = pay_df["payment_type"].map(lambda x: LABELS.get(x, x))
        fig = px.pie(
            pay_df, values="orders", names="label",
            hole=0.55, color_discrete_sequence=SEQ,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label",
                          textfont_size=11)
        fig.update_layout(**_layout("Payment Method Mix (by Order Count)", 420))
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 · CUSTOMER INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════
with TAB_CUSTOMERS:
    with st.spinner("Loading customer data…"):
        rfm_df   = q_rfm()
        state_df = q_customer_state()
        freq_df  = q_order_frequency()

    total_cust  = int(rfm_df["customers"].sum())
    champions   = int(rfm_df.loc[rfm_df["segment"] == "Champions", "customers"].sum())
    at_risk     = int(rfm_df.loc[rfm_df["segment"] == "At Risk",    "customers"].sum())
    inactive    = int(rfm_df.loc[rfm_df["segment"] == "Inactive",   "customers"].sum())
    active_rate = 1.0 - (inactive / total_cust) if total_cust else 0.0

    # ── KPI row ──────────────────────────────────────────────────────────────
    cx1, cx2, cx3, cx4 = st.columns(4)
    cx1.metric("Total Customers",    _num(total_cust))
    cx2.metric("Champions",          _num(champions),
               f"{champions / total_cust * 100:.1f}% of base")
    cx3.metric("At-Risk Customers",  _num(at_risk),
               f"{at_risk / total_cust * 100:.1f}%")
    cx4.metric("Active Customers",   _pct(active_rate))

    st.markdown("---")

    col_a, col_b, col_c = st.columns([1.2, 2, 1])

    with col_a:
        SEG_COLORS = {
            "Champions": PAL["green"],
            "Loyal":     PAL["teal"],
            "Promising": PAL["blue"],
            "At Risk":   PAL["amber"],
            "Inactive":  PAL["red"],
        }
        fig = px.pie(
            rfm_df, values="customers", names="segment",
            hole=0.55,
            color="segment",
            color_discrete_map=SEG_COLORS,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label",
                          textfont_size=10)
        fig.update_layout(**_layout("RFM Customer Segments", 400))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        fig = px.bar(
            state_df, x="customer_state", y="customers",
            color="customers",
            color_continuous_scale=["#1e3a8a", PAL["blue"], PAL["teal"]],
            labels={"customer_state": "State (BR)", "customers": "Unique Customers"},
        )
        fig.update_layout(**_layout("Unique Customers by State", 400))
        fig.update_coloraxes(showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_c:
        BUCKET_ORDER = ["1 order", "2 orders", "3 orders", "4+ orders"]
        freq_df["bucket"] = pd.Categorical(
            freq_df["bucket"], categories=BUCKET_ORDER, ordered=True
        )
        freq_df = freq_df.sort_values("bucket")
        fig = px.bar(
            freq_df, x="customers", y="bucket", orientation="h",
            color_discrete_sequence=[PAL["purple"]],
            labels={"customers": "Customers", "bucket": ""},
            text="customers",
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(**_layout("Purchase Frequency Distribution", 400))
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 · OPERATIONS & DELIVERY
# ══════════════════════════════════════════════════════════════════════════════
with TAB_OPS:
    with st.spinner("Loading operations data…"):
        del_state  = q_delivery_by_state()
        ontime_df  = q_monthly_ontime()
        status_df  = q_order_status()
        kpis       = q_overview_kpis().iloc[0]

    total_ord  = int(status_df["cnt"].sum())
    cancelled  = int(
        status_df.loc[status_df["order_status"] == "canceled", "cnt"].sum()
        if "canceled" in status_df["order_status"].values else 0
    )

    # ── KPI row ──────────────────────────────────────────────────────────────
    d1, d2, d3, d4 = st.columns(4)
    d1.metric("On-Time Delivery Rate", _pct(kpis["on_time_rate"]))
    d2.metric("Avg Delivery Days",     f"{kpis['avg_days']:.1f} days")
    d3.metric("Delivered Orders",      _num(kpis["delivered"]))
    d4.metric("Cancellation Rate",     _pct(cancelled / total_ord) if total_ord else "0%")

    st.markdown("---")

    col_x, col_y = st.columns([3, 2])

    with col_x:
        ds = del_state.sort_values("avg_days")
        bar_colors = [
            PAL["green"] if d <= 10
            else PAL["amber"] if d <= 15
            else PAL["red"]
            for d in ds["avg_days"]
        ]
        fig = go.Figure(go.Bar(
            x=ds["avg_days"],
            y=ds["state"],
            orientation="h",
            marker_color=bar_colors,
            text=ds["avg_days"].round(1).astype(str) + " days",
            textposition="outside",
            textfont=dict(size=11, color=FONT),
        ))
        fig.update_layout(
            **_layout("Avg Delivery Days by Customer State  🟢 ≤ 10  🟡 ≤ 15  🔴 > 15", 520),
        )
        fig.update_xaxes(title="Days", range=[0, ds["avg_days"].max() * 1.22])
        st.plotly_chart(fig, use_container_width=True)

    with col_y:
        fig2 = go.Figure(go.Scatter(
            x=ontime_df["month"],
            y=ontime_df["on_time_rate"] * 100,
            mode="lines+markers",
            line=dict(color=PAL["green"], width=2.5),
            marker=dict(size=4),
            fill="tozeroy",
            fillcolor="rgba(34,197,94,0.10)",
        ))
        fig2.update_layout(**_layout("Monthly On-Time Delivery Rate (%)", 250))
        fig2.update_yaxes(ticksuffix="%", range=[0, 115])
        fig2.add_hline(y=90, line_dash="dot", line_color="rgba(255,255,255,0.25)",
                       annotation_text="90% target", annotation_font_color=MUTED)
        st.plotly_chart(fig2, use_container_width=True)

        fig3 = px.pie(
            status_df, values="cnt", names="order_status",
            hole=0.55, color_discrete_sequence=SEQ,
        )
        fig3.update_traces(textposition="inside", textinfo="percent+label",
                           textfont_size=10)
        fig3.update_layout(**_layout("Order Status Breakdown", 260))
        st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 · PRODUCTS & SENTIMENT
# ══════════════════════════════════════════════════════════════════════════════
with TAB_PRODUCTS:
    with st.spinner("Loading product & sentiment data…"):
        rev_dist = q_review_distribution()
        sent_df  = q_sentiment_by_category()
        sent_df["category_type"] = sent_df["category_type"].map(_fmt_cat)
        rating_m = q_monthly_rating()
        score    = q_review_summary().iloc[0]
        n_prod   = q_total_products()

    positive_rate = (
        rev_dist.loc[rev_dist["review_score"] >= 4, "cnt"].sum()
        / rev_dist["cnt"].sum()
    )

    # ── KPI row ──────────────────────────────────────────────────────────────
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Total Products",      _num(n_prod))
    p2.metric("Avg Review Score",    f"{score['avg_score']:.2f} / 5 ★")
    p3.metric("Positive Review Rate", _pct(positive_rate))
    p4.metric("Total Reviews",       _num(score["total_reviews"]))

    st.markdown("---")

    col_1, col_2, col_3 = st.columns([1, 2, 1.2])

    with col_1:
        rev_dist["label"] = rev_dist["review_score"].astype(str) + " ★"
        STAR_COLORS = {
            "1 ★": PAL["red"],
            "2 ★": "#f97316",
            "3 ★": PAL["amber"],
            "4 ★": "#84cc16",
            "5 ★": PAL["green"],
        }
        fig = px.bar(
            rev_dist, x="cnt", y="label", orientation="h",
            color="label", color_discrete_map=STAR_COLORS,
            labels={"cnt": "Reviews", "label": "Score"},
            text="cnt",
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(**_layout("Review Score Distribution", 360))
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_2:
        fig2 = px.bar(
            sent_df,
            x="positive_rate",
            y="category_type",
            orientation="h",
            color="avg_score",
            color_continuous_scale=["#ef4444", PAL["amber"], PAL["green"]],
            range_color=[3.0, 5.0],
            labels={
                "positive_rate": "Positive Review Rate",
                "category_type": "Category",
                "avg_score": "Avg Score",
            },
            text=sent_df["positive_rate"].map(lambda x: f"{x*100:.0f}%"),
        )
        fig2.update_traces(textposition="outside")
        fig2.update_xaxes(tickformat=".0%", range=[0, 1.08])
        fig2.update_layout(**_layout("Positive Review Rate by Category Type", 360))
        fig2.update_coloraxes(
            colorbar=dict(
                title=dict(text="Avg ★", font=dict(color=MUTED)),
                tickfont=dict(color=MUTED),
                bgcolor=BG,
            )
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col_3:
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=rating_m["month"],
            y=rating_m["avg_score"],
            mode="lines+markers",
            line=dict(color=PAL["amber"], width=2.5),
            marker=dict(size=4),
            fill="tozeroy",
            fillcolor="rgba(245,158,11,0.10)",
        ))
        fig3.add_hline(
            y=4.0, line_dash="dot",
            line_color="rgba(255,255,255,0.25)",
            annotation_text="Score 4.0",
            annotation_font_color=MUTED,
        )
        fig3.update_layout(**_layout("Monthly Avg Review Score", 360))
        fig3.update_yaxes(range=[1, 5.5], title="Avg Score")
        st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — GEOGRAPHY & LOGISTICS
# ══════════════════════════════════════════════════════════════════════════════
with TAB_GEO:
    cust_map  = q_customer_order_map()
    sell_map  = q_seller_map()
    region_df = q_regional_performance()

    # ── KPI row ───────────────────────────────────────────────────────────────
    g1, g2, g3, g4 = st.columns(4)
    regions_covered = region_df["region"].nunique() if not region_df.empty else 0
    states_covered  = cust_map["state"].nunique()   if not cust_map.empty  else 0
    cities_covered  = cust_map["city"].nunique()    if not cust_map.empty  else 0

    # Delivery gap: North vs Southeast (worst vs best)
    if not region_df.empty and len(region_df) >= 2:
        max_days = region_df["avg_delivery_days"].max()
        min_days = region_df["avg_delivery_days"].min()
        delivery_gap = max_days - min_days
    else:
        delivery_gap = 0.0

    g1.metric("Regions Covered",    str(regions_covered))
    g2.metric("States with Orders", str(states_covered))
    g3.metric("Cities with Orders", f"{cities_covered:,}")
    g4.metric("Max–Min Delivery Gap", f"{delivery_gap:.1f} days",
              help="Difference in avg delivery days between slowest and fastest region")

    st.markdown("---")

    # ── Demand density heatmap ────────────────────────────────────────────────
    if not cust_map.empty:
        fig_density = px.density_mapbox(
            cust_map, lat="lat", lon="lng", z="orders",
            radius=18,
            hover_data={"city": True, "state": True, "orders": True,
                        "revenue": ":.0f", "lat": False, "lng": False},
            color_continuous_scale=["#0f172a", PAL["blue"], PAL["teal"], "#f0fdf4"],
            labels={"orders": "Orders", "revenue": "Revenue (R$)"},
        )
        fig_density.update_layout(
            mapbox_style="carto-darkmatter",
            mapbox_center={"lat": -14.2, "lon": -51.9},
            mapbox_zoom=3,
            paper_bgcolor=BG,
            margin=dict(l=0, r=0, t=36, b=0),
            height=500,
            title=dict(text="Customer Order Demand Density", font=dict(color=FONT, size=13)),
            coloraxis_colorbar=dict(
                title=dict(text="Orders", font=dict(color=MUTED)),
                tickfont=dict(color=MUTED),
                bgcolor=BG,
            ),
        )
        st.plotly_chart(fig_density, use_container_width=True)

    st.markdown("---")

    # ── Coverage map + Regional performance ───────────────────────────────────
    col_map, col_reg = st.columns([3, 2])

    with col_map:
        fig_cov = go.Figure()
        if not cust_map.empty:
            # Normalise marker size to 4–20px range
            max_orders = cust_map["orders"].max() or 1
            sizes = (cust_map["orders"] / max_orders * 16 + 4).clip(4, 20)
            fig_cov.add_trace(go.Scattermapbox(
                lat=cust_map["lat"], lon=cust_map["lng"],
                mode="markers",
                marker=dict(size=sizes, color=PAL["blue"], opacity=0.55),
                text=cust_map["city"] + ", " + cust_map["state"]
                     + "<br>Orders: " + cust_map["orders"].astype(str),
                hoverinfo="text",
                name="Customers",
            ))
        if not sell_map.empty:
            fig_cov.add_trace(go.Scattermapbox(
                lat=sell_map["lat"], lon=sell_map["lng"],
                mode="markers",
                marker=dict(size=7, color=PAL["amber"], opacity=0.75),
                text=sell_map["city"] + ", " + sell_map["state"]
                     + "<br>Sellers: " + sell_map["seller_count"].astype(str),
                hoverinfo="text",
                name="Sellers",
            ))
        fig_cov.update_layout(
            mapbox_style="carto-darkmatter",
            mapbox_center={"lat": -14.2, "lon": -51.9},
            mapbox_zoom=3,
            paper_bgcolor=BG,
            margin=dict(l=0, r=0, t=36, b=30),
            height=550,
            title=dict(text="Customer Demand vs Seller Supply Coverage",
                       font=dict(color=FONT, size=13)),
            legend=dict(bgcolor=BG, font=dict(color=FONT, size=11),
                        x=0.01, y=0.99),
        )
        st.plotly_chart(fig_cov, use_container_width=True)

    with col_reg:
        if not region_df.empty:
            fig_reg = go.Figure()
            fig_reg.add_trace(go.Bar(
                x=region_df["region"], y=region_df["revenue"],
                name="Revenue (R$)",
                marker_color=PAL["blue"],
                opacity=0.85,
                yaxis="y1",
            ))
            fig_reg.add_trace(go.Scatter(
                x=region_df["region"], y=region_df["avg_delivery_days"],
                name="Avg Delivery Days",
                mode="lines+markers",
                line=dict(color=PAL["amber"], width=2.2),
                marker=dict(size=6),
                yaxis="y2",
            ))
            fig_reg.update_layout(**_layout("Revenue & Delivery Days by Region", 420))
            fig_reg.update_layout(
                yaxis=dict(title="Revenue (R$)", gridcolor=GRID,
                           tickprefix="R$", tickformat=",.0f", tickfont=dict(color=MUTED)),
                yaxis2=dict(title="Avg Delivery Days", overlaying="y", side="right",
                            gridcolor="rgba(0,0,0,0)", tickfont=dict(color=MUTED)),
                barmode="overlay",
                legend=dict(x=0.01, y=0.99, bgcolor=BG),
            )
            st.plotly_chart(fig_reg, use_container_width=True)
