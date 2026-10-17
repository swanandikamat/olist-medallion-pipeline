# Databricks notebook source
# DBTITLE 1,Monitoring overview
# MAGIC %md
# MAGIC # 📋 Pipeline Monitoring (manual, Free Edition Serverless)
# MAGIC
# MAGIC This notebook is the **manually runnable fallback monitor** — no schedule, no deployment required.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Creates `pipeline_run_log` (once) — the *missing* run-status table that `monitoring/pipeline_monitoring.sql` assumed existed
# MAGIC 2. Records **one snapshot row per pipeline layer**: run timestamp, layer, row count, and data-quality outcome (from the real `data_quality_logs` table written by the Bronze/Silver/Gold quality gates)
# MAGIC 3. Displays a monitoring dashboard: run history, DQ outcomes, table freshness, current row counts
# MAGIC
# MAGIC **What it does NOT do (honest limits)**
# MAGIC * It records a snapshot **when you run it** — it does NOT capture notebook/job runs automatically (that would require wiring it into each notebook or the Job).
# MAGIC * Nothing here is scheduled. See the last cell for how to add a schedule to the existing Job if you want one.
# MAGIC
# MAGIC **Safe to rerun** — it only appends one snapshot row per run; it never modifies pipeline tables.

# COMMAND ----------

# DBTITLE 1,Create pipeline_run_log
# Step 1 — create pipeline_run_log once (the table monitoring/pipeline_monitoring.sql was missing)
spark.sql("""
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_ts          TIMESTAMP,  -- when this monitoring snapshot was taken
    layer           STRING,     -- bronze / silver / gold
    primary_table   STRING,    -- the layer's main table
    row_count       BIGINT,    -- rows in that table right now
    dq_checks       INT,       -- quality checks recorded in data_quality_logs
    dq_passed       INT,
    dq_failed       INT,
    dq_blocking     INT,       -- failed checks with severity='fail' (would stop a notebook)
    status          STRING     -- OK / WARN / FAIL
)
""")
print("pipeline_run_log ready:", spark.table("pipeline_run_log").count(), "existing rows")

# COMMAND ----------

# DBTITLE 1,Record run snapshot
# Step 2 — record one snapshot row per layer (row counts from real tables,
# DQ outcomes from the real data_quality_logs written by the quality gates)
from pyspark.sql import functions as F

LAYERS = {
    "bronze": "bronze_orders",
    "silver": "silver_order_complete",
    "gold":   "gold_product_sales",
}

# Aggregate the latest DQ-gate results per layer from data_quality_logs
# (bronze gate logs bronze_* tables, silver gate logs silver_*, gold gate logs gold_*)
dq = spark.table("data_quality_logs")
dq_by_layer = (
    dq.withColumn("layer",
        F.when(F.col("table_name").startswith("bronze"), "bronze")
         .when(F.col("table_name").startswith("silver"), "silver")
         .when(F.col("table_name").startswith("gold"), "gold"))
    .groupBy("layer")
    .agg(F.count("*").alias("dq_checks"),
         F.sum(F.col("passed").cast("int")).alias("dq_passed"),
         F.sum((~F.col("passed")).cast("int")).alias("dq_failed"),
         F.sum(((~F.col("passed")) & (F.col("severity") == "fail")).cast("int")).alias("dq_blocking"))
)
dq_map = {r["layer"]: r for r in dq_by_layer.collect()}

run_ts = F.current_timestamp()
rows = []
for layer, table in LAYERS.items():
    n = spark.table(table).count()
    d = dq_map.get(layer)
    checks = d["dq_checks"] if d else 0
    passed = d["dq_passed"] if d else 0
    failed = d["dq_failed"] if d else 0
    blocking = d["dq_blocking"] if d else 0
    status = "FAIL" if blocking > 0 else ("WARN" if failed > 0 else "OK")
    rows.append((layer, table, n, checks, passed, failed, blocking, status))

snapshot = spark.createDataFrame(
    rows, ["layer", "primary_table", "row_count", "dq_checks", "dq_passed", "dq_failed", "dq_blocking", "status"]
).withColumn("run_ts", run_ts).select("run_ts", "layer", "primary_table", "row_count",
                                     "dq_checks", "dq_passed", "dq_failed", "dq_blocking", "status")
snapshot.write.mode("append").saveAsTable("pipeline_run_log")
print(f"✅ Recorded snapshot for {len(rows)} layers at {spark.sql('SELECT current_timestamp() ts').collect()[0]['ts']}")
snapshot.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Monitoring dashboard
# Step 3 — monitoring dashboard (all queries reference REAL tables only)

# 3a. Run history from pipeline_run_log (this notebook's snapshots)
print("📋 Run history (pipeline_run_log)")
display(spark.sql("""
    SELECT run_ts, layer, primary_table, row_count,
           dq_checks, dq_passed, dq_failed, dq_blocking, status
    FROM pipeline_run_log
    ORDER BY run_ts DESC, layer
    LIMIT 15
"""))

# 3b. Data-quality outcomes by table (real table: data_quality_logs)
print("\n🧪 Data quality by table (data_quality_logs, all recorded gates)")
display(spark.sql("""
    SELECT table_name, check_name, severity, passed, timestamp
    FROM data_quality_logs
    WHERE passed = false
    ORDER BY timestamp DESC
"""))
print("↑ only failed checks are shown; empty result = every recorded check passed")

# 3c. Load freshness — bronze only: silver/gold have NO ingestion_time column
#     (documented Phase 6 finding; for silver/gold, row-count history in 3a is the proxy)
print("\n🕒 Bronze load freshness (bronze_orders.ingestion_time)")
display(spark.sql("""
    SELECT MAX(ingestion_time) AS latest_ingestion,
           ROUND(DATEDIFF(HOUR, MAX(ingestion_time), CURRENT_TIMESTAMP()), 1) AS hours_since,
           CASE WHEN DATEDIFF(HOUR, MAX(ingestion_time), CURRENT_TIMESTAMP()) > 24 THEN 'STALE'
                ELSE 'FRESH' END AS freshness
    FROM bronze_orders
"""))

# COMMAND ----------

# DBTITLE 1,Validation and scheduling notes
# MAGIC %md
# MAGIC ## Validation steps (exact)
# MAGIC
# MAGIC 1. **Run All** in this notebook — expect: `pipeline_run_log ready: N existing rows`, then `✅ Recorded snapshot for 3 layers`, then three dashboard tables.
# MAGIC 2. Verify the snapshot landed: `SELECT * FROM pipeline_run_log ORDER BY run_ts DESC LIMIT 3` — should show 3 rows (bronze/silver/gold) with your run timestamp.
# MAGIC 3. Cross-check a row count: `SELECT COUNT(*) FROM bronze_orders` should equal the bronze `row_count` in the snapshot.
# MAGIC 4. DQ columns should tie out to `data_quality_logs` (17 rows as of the last gate run; bronze_review has known duplicate warnings → bronze status shows WARN, which is **expected and correct**).
# MAGIC 5. Rerun the notebook — expect 3 *additional* rows (append-only, never modifies pipeline tables).
# MAGIC
# MAGIC ## Scheduling — current state, honestly
# MAGIC
# MAGIC * **No schedule is active.** The existing Job “Ecommerce Data Pipeline” (5 tasks: Ingestion→Bronze→Silver→Gold→SQL_analysis) has only been run **manually** (all runs are `ONE_TIME`).
# MAGIC * The `databricks.yml` bundle defines a 2 AM cron schedule — but the bundle has **never been deployed** (no bundle-created job exists), so that schedule is **not** in effect. Don't rely on it.
# MAGIC * **If you want real scheduling:** add a schedule to the *existing* Job (Jobs UI → your job → Add schedule) — Jobs run fine on Free Edition Serverless (verified: 2 jobs exist and have run). Wiring this monitoring notebook as a final task of that Job would then record a snapshot on every scheduled run — no Asset Bundle needed.