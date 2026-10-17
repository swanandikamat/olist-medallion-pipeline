# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # 🐍 Python Analytics on Gold Tables
# MAGIC
# MAGIC This notebook turns the Gold-layer tables into readable charts and a KPI summary using **PySpark + Pandas + Matplotlib**.
# MAGIC
# MAGIC **Data flow:** Gold Delta tables (PySpark) → small aggregated results (Pandas) → charts (Matplotlib) → PNG files.
# MAGIC
# MAGIC All numbers are read **live from the tables** — nothing is hardcoded.

# COMMAND ----------

# DBTITLE 1,How it works
# MAGIC %md
# MAGIC ## How this notebook works
# MAGIC
# MAGIC * **PySpark does the heavy lifting.** All aggregation, filtering, and top-N selection happens in Spark — the Gold tables contain up to ~97k rows.
# MAGIC * **Pandas only sees small results.** A helper (`to_small_pandas`) refuses to convert anything bigger than 5,000 rows, so we never accidentally pull a huge table to the driver.
# MAGIC * **Matplotlib draws the charts** with a single restrained blue palette, clear titles, labeled axes, units (R$), and readable date formatting.
# MAGIC * **Every chart is saved as a PNG** to `Project/analytics_charts/` and also shown inline.
# MAGIC * **Empty tables and nulls are handled** — the helper warns and skips a chart instead of crashing.
# MAGIC * Each chart cell ends with a small **self-test** (assertions), and a final cell verifies all PNG files exist.

# COMMAND ----------

# DBTITLE 1,Setup and helpers
# Imports, chart folder, and shared helper functions
import os
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mtick
import pandas as pd
from pyspark.sql import functions as F

# Folder where chart PNGs are saved (created if missing)
CHART_DIR = "/Workspace/Users/swanandikamat18@gmail.com/Project/analytics_charts"
os.makedirs(CHART_DIR, exist_ok=True)

# Restrained professional palette: one blue accent
ACCENT = "#2171b5"
MUTED = "#9ecae1"


def to_small_pandas(sdf, name, max_rows=5000):
    """Convert a SMALL Spark result to pandas, safely.

    Always aggregate / limit in Spark FIRST, then call this.
    Handles empty tables and null values; returns None when
    there is nothing to chart.
    """
    n = sdf.count()
    if n == 0:
        print(f"⚠️ {name}: table is empty — nothing to chart.")
        return None
    if n > max_rows:
        raise ValueError(
            f"{name} has {n:,} rows — too big for pandas. "
            "Aggregate or .limit() in Spark first.")
    pdf = sdf.toPandas()
    nulls = int(pdf.isna().sum().sum())
    if nulls:
        print(f"⚠️ {name}: {nulls} null value(s) found — dropped for the chart.")
        pdf = pdf.dropna()
        if pdf.empty:
            print(f"⚠️ {name}: empty after dropping nulls — nothing to chart.")
            return None
    print(f"✅ {name}: {len(pdf):,} rows ready for charting")
    return pdf


def style_axes(ax, title, xlabel="", ylabel=""):
    """Apply the shared professional styling to one chart."""
    ax.set_title(title, fontsize=13, loc="left", pad=12)
    ax.set_xlabel(xlabel, fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="x", alpha=0.25, linestyle="--")
    ax.tick_params(labelsize=9)


def save_chart(fig, filename):
    """Save a chart as PNG and preview it inline (best effort)."""
    path = os.path.join(CHART_DIR, filename)
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"💾 saved: {path}")
    try:
        from IPython.display import Image as IPImage, display as ipy_display
        ipy_display(IPImage(filename=path))
    except Exception as e:
        print(f"(inline preview unavailable: {e} — PNG is saved at {path})")
    return path


print("Setup complete — charts will be saved to:", CHART_DIR)

# COMMAND ----------

# DBTITLE 1,KPI section header
# MAGIC %md
# MAGIC ## 📊 KPI Summary
# MAGIC
# MAGIC All KPI values below are read **live from `gold_dashboard_summary`** and cross-checked against the other Gold tables — no hardcoded numbers.
# MAGIC
# MAGIC **Definitions (matching the validated Gold build):**
# MAGIC
# MAGIC * **Total orders** — distinct `order_id` values that have at least one item row in the Gold fact table (`silver_order_complete`).
# MAGIC * **Total customers** — distinct `customer_id` for those orders (in Olist every order has its own `customer_id`, so this equals the order count).
# MAGIC * **Total revenue** — **item revenue**: sum of `price × quantity` over those orders (freight is **not** included).
# MAGIC * **Average order value (AOV)** — total revenue ÷ total orders.
# MAGIC
# MAGIC ⚠️ **Known caveats (from the Phase 1 audit, fixes pending approval):**
# MAGIC
# MAGIC * The Gold fact table currently includes **all order statuses** (including `canceled` and `unavailable`).
# MAGIC * The stored **AOV** in `gold_dashboard_summary` divides revenue by *item rows* instead of *distinct orders*, so it understates the true AOV — the notebook shows both the stored value and the recomputed `revenue / orders` value.

# COMMAND ----------

# DBTITLE 1,KPI summary (live from Gold)
# KPI summary — read live from gold_dashboard_summary + cross-checks
summary_pdf = to_small_pandas(spark.table("gold_dashboard_summary"), "gold_dashboard_summary")

if summary_pdf is not None:
    kpi = summary_pdf.iloc[0]  # single-row table

    # Live cross-checks (computed from the other Gold tables, not hardcoded)
    monthly_total = spark.table("gold_monthly_sales").agg(F.sum("monthly_revenue")).collect()[0][0]
    daily_total = spark.table("gold_daily_sales").agg(F.sum("daily_revenue")).collect()[0][0]
    aov_recomputed = kpi["revenue"] / kpi["orders"]  # definition: revenue / orders

    print("=" * 62)
    print("KPI SUMMARY — Olist E-commerce (Gold layer)")
    print("=" * 62)
    print(f"Total orders:        {kpi['orders']:>12,.0f}  orders with ≥1 item row")
    print(f"Total customers:     {kpi['customers']:>12,.0f}  distinct customer_id")
    print(f"Total revenue:       R$ {kpi['revenue']:>11,.2f}  (item revenue, excl. freight)")
    print(f"Average order value: R$ {kpi['average_order']:>11,.2f}  (as stored in the table)")
    print(f"AOV (recomputed):     R$ {aov_recomputed:>11,.2f}  (revenue / orders — definition)")
    print("-" * 62)
    print(f"Cross-check Σ monthly revenue: R$ {monthly_total:,.2f}")
    print(f"Cross-check Σ daily revenue:   R$ {daily_total:,.2f}")

    # Self-test: revenue must reconcile with the aggregated Gold tables.
    # NOTE: the stored AOV is NOT asserted equal to revenue/orders — that is
    # a known Gold-build bug from the Phase 1 audit (the Gold notebook divides
    # by item rows instead of distinct orders). It is reported, not asserted,
    # until the approved fix is applied to 04_Gold.
    assert abs(kpi["revenue"] - monthly_total) < 0.01, "summary revenue ≠ Σ monthly revenue"
    assert abs(kpi["revenue"] - daily_total) < 0.01, "summary revenue ≠ Σ daily revenue"
    if abs(kpi["average_order"] - aov_recomputed) >= 0.01:
        print(f"⚠️ Known Phase 1 finding: stored AOV ({kpi['average_order']:.2f}) differs "
              f"from revenue/orders ({aov_recomputed:.2f}) — Gold AOV fix pending approval.")
    print("✅ KPI self-test PASSED: revenue reconciles with monthly/daily aggregates")

# COMMAND ----------

# DBTITLE 1,Chart 1 header
# MAGIC %md
# MAGIC ## 📈 Chart 1 — Monthly Revenue Trend
# MAGIC
# MAGIC Source: `gold_monthly_sales` (one row per month, `YYYY-MM` string, ~24 rows — tiny, safe for pandas).
# MAGIC
# MAGIC We plot revenue in **R$ millions** so the axis stays readable. Months sort correctly as strings (`"2016-09" < "2017-01"`).

# COMMAND ----------

# DBTITLE 1,Chart 1 code
# Chart 1: Monthly revenue trend
monthly_pdf = to_small_pandas(
    spark.table("gold_monthly_sales").orderBy("month"), "gold_monthly_sales")

if monthly_pdf is not None:
    fig, ax = plt.subplots(figsize=(10, 4.5))
    ax.plot(monthly_pdf["month"], monthly_pdf["monthly_revenue"] / 1_000_000,
            color=ACCENT, linewidth=2, marker="o", markersize=4)
    style_axes(ax,
               "Monthly Revenue Trend — Olist E-commerce (2016–2018)",
               "Month", "Revenue (R$ million)")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v:.1f}M"))
    ax.tick_params(axis="x", rotation=45)
    ax.grid(axis="x", alpha=0)
    ax.grid(axis="y", alpha=0.25, linestyle="--")
    monthly_chart = save_chart(fig, "01_monthly_revenue_trend.png")

    # Self-test: chart data is non-empty, positive, and the PNG was created
    assert len(monthly_pdf) > 0, "monthly chart: no data"
    assert (monthly_pdf["monthly_revenue"] > 0).all(), "monthly chart: non-positive revenue"
    assert os.path.getsize(monthly_chart) > 0, "monthly chart: empty PNG"
    print("✅ Chart 1 self-test PASSED")

# COMMAND ----------

# DBTITLE 1,Chart 2 header
# MAGIC %md
# MAGIC ## 📈 Chart 2 — Daily Sales Trend
# MAGIC
# MAGIC Source: `gold_daily_sales` (615 rows — one row per day with sales). Daily numbers are noisy, so we add a **30-day rolling average** to make the trend readable. The x-axis uses real dates formatted `Mon YYYY`.

# COMMAND ----------

# DBTITLE 1,Chart 2 code
# Chart 2: Daily sales trend (raw + 30-day rolling average)
daily_pdf = to_small_pandas(
    spark.table("gold_daily_sales").orderBy("order_date"), "gold_daily_sales")

if daily_pdf is not None:
    # Ensure real datetime values for date formatting (handles nulls via helper)
    daily_pdf["order_date"] = pd.to_datetime(daily_pdf["order_date"])
    daily_pdf = daily_pdf.sort_values("order_date")
    daily_pdf["rolling_30d"] = daily_pdf["daily_revenue"].rolling(window=30, min_periods=1).mean()

    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.plot(daily_pdf["order_date"], daily_pdf["daily_revenue"],
            color=MUTED, linewidth=0.8, alpha=0.6, label="Daily revenue")
    ax.plot(daily_pdf["order_date"], daily_pdf["rolling_30d"],
            color=ACCENT, linewidth=2.2, label="30-day rolling average")
    style_axes(ax,
               "Daily Sales Trend — Olist E-commerce (2016–2018)",
               "Order date", "Revenue (R$ thousand)")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v/1_000:.0f}k"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.tick_params(axis="x", rotation=45)
    ax.legend(frameon=False, fontsize=9)
    daily_chart = save_chart(fig, "02_daily_sales_trend.png")

    # Self-test: sorted dates, non-empty PNG, rolling avg computed
    assert daily_pdf["order_date"].is_monotonic_increasing, "daily chart: dates not sorted"
    assert daily_pdf["rolling_30d"].notna().all(), "daily chart: rolling average has nulls"
    assert os.path.getsize(daily_chart) > 0, "daily chart: empty PNG"
    print("✅ Chart 2 self-test PASSED")

# COMMAND ----------

# DBTITLE 1,Chart 3 header
# MAGIC %md
# MAGIC ## 🏆 Chart 3 — Top 10 Products by Revenue
# MAGIC
# MAGIC Source: `gold_product_sales` (32,341 rows — **too big for pandas as-is**). We take the **top 10 in Spark** (`orderBy + limit`) and only then convert — exactly the pattern the helper enforces. Product IDs are 32-char hashes, so labels are shortened to the first 10 characters for readability.

# COMMAND ----------

# DBTITLE 1,Chart 3 code
# Chart 3: Top 10 products by revenue (top-N selected in Spark, not pandas)
top_products_sdf = (
    spark.table("gold_product_sales")
    .orderBy(F.col("revenue").desc())
    .limit(10)
)
top_products_pdf = to_small_pandas(top_products_sdf, "top 10 products")

if top_products_pdf is not None:
    # Short labels for the 32-char product hashes
    top_products_pdf["label"] = top_products_pdf["product_id"].str[:10] + "…"
    top_products_pdf = top_products_pdf.sort_values("revenue")  # barh reads bottom-up

    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.barh(top_products_pdf["label"], top_products_pdf["revenue"], color=ACCENT)
    style_axes(ax,
               "Top 10 Products by Revenue — Olist E-commerce",
               "Revenue (R$)", "Product (first 10 chars of ID)")
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v/1_000:.0f}k"))
    products_chart = save_chart(fig, "03_top_10_products.png")

    # Self-test: exactly 10 rows, sorted descending, valid PNG
    assert len(top_products_pdf) == 10, "products chart: expected 10 rows"
    rev = top_products_pdf["revenue"].tolist()[::-1]  # back to descending
    assert rev == sorted(rev, reverse=True), "products chart: not sorted by revenue"
    assert os.path.getsize(products_chart) > 0, "products chart: empty PNG"
    print("✅ Chart 3 self-test PASSED")

# COMMAND ----------

# DBTITLE 1,Chart 4 header
# MAGIC %md
# MAGIC ## 🏆 Chart 4 — Top 10 Product Categories by Revenue
# MAGIC
# MAGIC Source: `gold_category_sales` (73 rows). Categories use the original **Portuguese** names from Olist (e.g. `cama_mesa_banho` = bed/table/bath) — kept as-is for traceability.

# COMMAND ----------

# DBTITLE 1,Chart 4 code
# Chart 4: Top 10 categories by revenue
top_categories_sdf = (
    spark.table("gold_category_sales")
    .orderBy(F.col("revenue").desc())
    .limit(10)
)
top_categories_pdf = to_small_pandas(top_categories_sdf, "top 10 categories")

if top_categories_pdf is not None:
    top_categories_pdf = top_categories_pdf.sort_values("revenue")

    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.barh(top_categories_pdf["product_category_name"],
            top_categories_pdf["revenue"], color=ACCENT)
    style_axes(ax,
               "Top 10 Product Categories by Revenue — Olist E-commerce",
               "Revenue (R$ million)", "Category (Portuguese source names)")
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v/1e6:.1f}M"))
    categories_chart = save_chart(fig, "04_top_10_categories.png")

    # Self-test
    assert len(top_categories_pdf) == 10, "categories chart: expected 10 rows"
    assert (top_categories_pdf["revenue"] > 0).all(), "categories chart: non-positive revenue"
    assert os.path.getsize(categories_chart) > 0, "categories chart: empty PNG"
    print("✅ Chart 4 self-test PASSED")

# COMMAND ----------

# DBTITLE 1,Chart 5 header
# MAGIC %md
# MAGIC ## 🏆 Chart 5 — Top 10 Cities by Revenue
# MAGIC
# MAGIC Source: `gold_city_sales` (4,095 rows — top-N again selected in Spark first). City names are lower-case in the source, so we `str.title()` them for readable labels.

# COMMAND ----------

# DBTITLE 1,Chart 5 code
# Chart 5: Top 10 cities by revenue (top-N selected in Spark first)
top_cities_sdf = (
    spark.table("gold_city_sales")
    .orderBy(F.col("city_revenue").desc())
    .limit(10)
)
top_cities_pdf = to_small_pandas(top_cities_sdf, "top 10 cities")

if top_cities_pdf is not None:
    top_cities_pdf["city"] = top_cities_pdf["customer_city"].str.title()
    top_cities_pdf = top_cities_pdf.sort_values("city_revenue")

    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.barh(top_cities_pdf["city"], top_cities_pdf["city_revenue"], color=ACCENT)
    style_axes(ax,
               "Top 10 Cities by Revenue — Olist E-commerce",
               "Revenue (R$ million)", "City")
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda v, _: f"{v/1e6:.1f}M"))
    cities_chart = save_chart(fig, "05_top_10_cities.png")

    # Self-test
    assert len(top_cities_pdf) == 10, "cities chart: expected 10 rows"
    assert (top_cities_pdf["city_revenue"] > 0).all(), "cities chart: non-positive revenue"
    assert os.path.getsize(cities_chart) > 0, "cities chart: empty PNG"
    print("✅ Chart 5 self-test PASSED")

# COMMAND ----------

# DBTITLE 1,Final chart validation test
# Final test: verify every expected chart PNG exists and is non-empty
expected_charts = [
    "01_monthly_revenue_trend.png",
    "02_daily_sales_trend.png",
    "03_top_10_products.png",
    "04_top_10_categories.png",
    "05_top_10_cities.png",
]

print("=" * 62)
print("Final chart validation")
print("=" * 62)
all_ok = True
for filename in expected_charts:
    path = os.path.join(CHART_DIR, filename)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        size_kb = os.path.getsize(path) / 1024
        print(f"✅ {filename} ({size_kb:.0f} KB)")
    else:
        print(f"❌ {filename} MISSING or empty")
        all_ok = False

assert all_ok, "Some charts are missing — run the chart cells above"
print()
print("🎉 All 5 charts saved successfully to:", CHART_DIR)

# COMMAND ----------

# DBTITLE 1,Recap and export notes
# MAGIC %md
# MAGIC ## 📁 Where the charts live & how to reuse them
# MAGIC
# MAGIC All PNGs are saved in **`Project/analytics_charts/`** (created automatically) — you can download them from the workspace file browser, embed them in the project README, or attach them to a portfolio write-up.
# MAGIC
# MAGIC **Notes for reruns**
# MAGIC
# MAGIC * This notebook is **read-only** against the Gold tables — it never writes back to Delta, so it is always safe to rerun.
# MAGIC * Values are recomputed from the tables on every run — if Silver/Gold are rebuilt, charts and KPIs update automatically.
# MAGIC * If a table were empty, the helper prints a warning and skips that chart instead of crashing.

# COMMAND ----------

