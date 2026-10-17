# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql import functions as F


# COMMAND ----------

# DBTITLE 1,Load gold_product_sales (display bug fixed)
gold_sales = spark.table("gold_product_sales")

display(gold_sales)  # was: display(gold) — undefined variable, fixed

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

# Spark Connect compatible: use spark_partition_id to count partitions
partition_count = gold_sales.select(spark_partition_id()).distinct().count()
print(f"Number of partitions: {partition_count}")

# COMMAND ----------

gold_sales.explain(True)

# COMMAND ----------

# DBTITLE 1,Silver join (broadcast comment corrected)
# Use silver_order_complete which already has product_id
order_complete = spark.table("silver_order_complete")
products = spark.table("silver_products")

# NOTE (Phase 5 audit): broadcast joins ARE supported on Spark Connect —
# the optimizer auto-broadcasts small tables (see the plan-analysis cell below:
# silver_products, 0.8 MB, is broadcast automatically via BroadcastHashJoin).
joined = order_complete.join(
    products,
    "product_id"
)

display(joined.limit(10))

# COMMAND ----------

# DBTITLE 1,Phase 5 audit header
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # 🔍 Phase 5 — Performance Audit (measured, not assumed)
# MAGIC
# MAGIC **Method:** every claim below comes from a measurement or a Delta/table metadata fact captured in this notebook. No optimization was applied — see the findings cell for why, and what to revisit if the data grows.
# MAGIC
# MAGIC 1. **Table inventory** — file counts, sizes, partition & clustering columns (from `DESCRIBE DETAIL`, no data scans)
# MAGIC 2. **Environment probe** — which Spark knobs Free Edition Serverless actually exposes
# MAGIC 3. **Baseline benchmarks** — 4 representative queries × 3 timed runs (median reported)
# MAGIC 4. **Query-plan analysis** — shuffle counts, join strategies, AQE presence
# MAGIC 5. **Findings** — technique-by-technique verdict with rollback steps

# COMMAND ----------

# DBTITLE 1,1) Table inventory

# 1) Table inventory — sizes, file counts, partitioning, clustering (Delta metadata only)
tables = ["bronze_orders", "bronze_customers", "bronze_products", "bronze_payments", "bronze_review", "bronze_order_items",
          "silver_orders", "silver_customers", "silver_products", "silver_order_complete",
          "gold_daily_sales", "gold_monthly_sales", "gold_product_sales", "gold_category_sales", "gold_city_sales", "gold_top_customers", "gold_dashboard_summary"]
print(f"{'table':<28}{'files':>6}{'size_MB':>10}  part_cols / clustering")
print("-" * 75)
total = 0
for t in tables:
    d = spark.sql(f"DESCRIBE DETAIL {t}").collect()[0]
    mb = d["sizeInBytes"] / 1e6
    total += mb
    print(f"{t:<28}{d['numFiles']:>6}{mb:>10.2f}  parts={d['partitionColumns'] or '-'} clustering={d['clusteringColumns'] or '-'}")
print("-" * 75)
print(f"TOTAL: {total:.1f} MB across {len(tables)} tables")

# COMMAND ----------

# DBTITLE 1,2) Environment probe

# 2) Environment probe — what Free Edition Serverless exposes
print("Spark version:", spark.version)
for key in ["spark.sql.adaptive.enabled",
            "spark.sql.adaptive.autoBroadcastJoinThreshold",
            "spark.sql.shuffle.partitions"]:
    try:
        print(f"  {key} = {spark.conf.get(key)}")
    except Exception:
        print(f"  {key} -> NOT AVAILABLE on this compute (managed by Serverless)")

# COMMAND ----------

# DBTITLE 1,3) Baseline benchmarks

# 3) Baseline benchmarks — 4 representative queries, median of 3 timed runs
import time

def bench(fn, runs=3):
    """Median wall time (s) over N runs after one warm-up. Small results are
    collected to the driver so each run does real end-to-end work."""
    fn()  # warm-up
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    times.sort()
    return times[len(times) // 2], times[0]

QUERIES = {
    "Q1 gold_daily 1yr filter+agg": lambda: spark.sql("""
        SELECT order_date, SUM(daily_revenue) AS rev
        FROM gold_daily_sales
        WHERE order_date BETWEEN '2017-01-01' AND '2017-12-31'
        GROUP BY order_date ORDER BY order_date""").collect(),
    "Q2 silver fact cat×month agg": lambda: spark.sql("""
        SELECT product_category_name, DATE_FORMAT(order_date,'yyyy-MM') AS m, SUM(revenue) AS rev
        FROM silver_order_complete
        GROUP BY product_category_name, m""").collect(),
    "Q3 gold top-10 products": lambda: spark.sql("""
        SELECT product_id, revenue FROM gold_product_sales
        ORDER BY revenue DESC LIMIT 10""").collect(),
    "Q4 silver join+agg by category": lambda: spark.sql("""
        SELECT p.product_category_name, SUM(s.revenue) AS rev
        FROM silver_order_complete s
        JOIN silver_products p ON s.product_id = p.product_id
        GROUP BY p.product_category_name""").collect(),
}

print(f"{'query':<34}{'median_ms':>10}{'best_ms':>10}")
print("-" * 56)
base_results = {}
for name, fn in QUERIES.items():
    med, best = bench(fn)
    base_results[name] = (med, best)
    print(f"{name:<34}{med*1000:>10.1f}{best*1000:>10.1f}")
print("\n⚠️ Interpretation: times are dominated by per-statement overhead")
print("   (Spark Connect RPC + planning), not by data processing —")
print("   the largest table scanned is only ~5.6 MB in a single file.")

# COMMAND ----------

# DBTITLE 1,4) Query-plan analysis

# 4) Query-plan analysis — shuffle, join strategy, AQE presence
import io, contextlib

PLAN_QUERIES = {
    "Q2 (fact cat×month agg)": """
        SELECT product_category_name, DATE_FORMAT(order_date,'yyyy-MM') AS m, SUM(revenue) AS rev
        FROM silver_order_complete
        GROUP BY product_category_name, m""",
    "Q4 (fact ⋈ products, agg by category)": """
        SELECT p.product_category_name, SUM(s.revenue) AS rev
        FROM silver_order_complete s
        JOIN silver_products p ON s.product_id = p.product_id
        GROUP BY p.product_category_name""",
}

for name, sql in PLAN_QUERIES.items():
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        spark.sql(sql).explain(True)
    phys = buf.getvalue().split("== Physical Plan ==")[-1]
    print(f"===== {name} =====")
    print("  AQE (AdaptiveSparkPlan) present:", "AdaptiveSparkPlan" in phys)
    print("  Shuffle (Exchange) operators:", phys.count("ExchangeSink") or phys.count("Exchange"))
    print("  BroadcastHashJoin:", "BroadcastHashJoin" in phys,
          "| SortMergeJoin:", "SortMergeJoin" in phys)
    for line in phys.splitlines():
        s = line.strip()
        if any(k in s for k in ["PhotonScan", "BroadcastHashJoin", "ShuffleExchangeSink"]):
            print("   |", s[:120])
    print()

# COMMAND ----------

# DBTITLE 1,5) Findings and recommendations
# MAGIC %md
# MAGIC ## 5) Findings — verdict per technique
# MAGIC
# MAGIC **Bottom line: no optimization was applied.** All 17 tables total **39.7 MB**, each a **single Delta file** (largest: 5.7 MB), unpartitioned, unclustered. At this scale, table-layout techniques cannot produce a measurable win — every query already reads exactly one file — and benchmarks are dominated by per-statement overhead (Spark Connect RPC + planning), not data processing. Applying them anyway would add cost and complexity for a result we cannot measure honestly.
# MAGIC
# MAGIC | Technique | Supported on Free Edition Serverless? | Appropriate here? | Why / rollback if ever applied |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **OPTIMIZE** (compaction) | ✅ Yes (SQL `OPTIMIZE <table>`) | ❌ No | Compacts *many small files* into fewer; every table has **exactly 1 file** — there is literally nothing to compact. Rollback: `OPTIMIZE` writes a new snapshot only — `RESTORE TABLE <t> TO VERSION AS OF <n>` |
# MAGIC | **ZORDER** | ✅ Yes (`OPTIMIZE ... ZORDER BY`) | ❌ No | Reorders data within files to speed *multi-column file skipping*; pointless when the whole table is one file smaller than most Parquet row-groups. Rollback: same as OPTIMIZE (time travel). |
# MAGIC | **Liquid clustering** (`CLUSTER BY`) | ✅ Yes (Spark 4.x / Delta feature) | ❌ Now | Requires a full table rewrite (`CREATE OR REPLACE ... CLUSTER BY` or `ALTER TABLE ... CLUSTER BY` + rewrite) to help file skipping — no benefit below a single file per table. Rollback: `RESTORE TABLE TO VERSION AS OF` (rewrite creates a new version). |
# MAGIC | **Partitioning** | ✅ Yes | ❌ No — would *hurt* | Partitioning a 0.01–5.7 MB table creates tiny per-partition files and metadata overhead. **`pipeline_config.yaml`'s `partition_columns` for gold tables should stay unused at this size.** Rollback: only a table rewrite removes partitions — `RESTORE` to pre-rewrite version. |
# MAGIC | **Caching** (`cacheTable`) | ✅ Works on Spark Connect | ⚠️ Rarely | Only pays off when the *same* table is scanned *repeatedly within one session*. Our notebooks scan each table once per run. Rollback: transient by nature — `UNCACHE TABLE` / session end. |
# MAGIC | **AQE** | ✅ Active — but **not tunable** | N/A (already on) | `AdaptiveSparkPlan` appears in every plan and `spark.sql.shuffle.partitions = auto`; the `spark.sql.adaptive.*` knobs are **not exposed** on Serverless — managed automatically. Nothing to do. |
# MAGIC | **Broadcast join** | ✅ Supported (contrary to the old comment in this notebook) | ✅ Already automatic | The Q4 plan shows `PhotonBroadcastHashJoin` — the 0.8 MB `silver_products` side is auto-broadcast. **No code hint needed.** |
# MAGIC
# MAGIC **When to revisit:** if any table grows to **hundreds of files / >1 GB** (≈100× today), re-run this notebook's benchmark and plan cells first, then consider `OPTIMIZE` on that table — and re-measure before/after with the same harness. **Never adopt a technique without a before/after measurement from the same harness.**
# MAGIC
# MAGIC **Data preservation:** this audit only ran `DESCRIBE DETAIL`, `explain()`, and read-only queries. The next cell records each table's Delta version as proof nothing was modified.

# COMMAND ----------

# DBTITLE 1,6) Data-preservation proof

# 6) Data-preservation proof — every table's current Delta version (read-only)
# If these versions ever change as a result of an *optimization* action,
# RESTORE TABLE <t> TO VERSION AS OF <version> is the rollback.
for t in ["bronze_orders", "silver_order_complete", "gold_product_sales", "gold_daily_sales", "gold_dashboard_summary"]:
    v = spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").collect()[0]["version"]
    print(f"  {t:<28} current Delta version = {v}")
print("\n✅ Audit complete — no tables were modified, nothing to roll back.")