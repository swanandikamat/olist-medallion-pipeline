# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

silver = spark.table("silver_order_complete")

# COMMAND ----------

# DBTITLE 1,Gold Input Quality Gate
# ── Data Quality Gate: Gold inputs ─────────────────────────────────────
# Blocks the Gold stage if the silver input is not usable, and (advisory)
# reconciles per-order payments against item price + freight.
# NOTE: item revenue (price x quantity, no freight) and payment totals
# (freight included) are DIFFERENT metrics — never treat them as interchangeable.
import sys
sys.path.append("/Workspace/Users/swanandikamat18@gmail.com/Project/data_quality")
from quality_checks import DataQualityChecker

qc = DataQualityChecker(spark)

qc.check_financial_reconciliation(
    spark.table("bronze_order_items"),
    spark.table("bronze_payments"),
    order_id="order_id", price_col="price", freight_col="freight_value",
    payment_col="payment_value", max_mismatch_pct=1.0,
    table_name="bronze_items_vs_payments", severity="warn")

silver_in = spark.table("silver_order_complete")
qc.check_value_range(silver_in, "revenue", min_value=0,
                     table_name="silver_order_complete", severity="fail")
qc.check_null_keys(silver_in, ["order_id", "product_id"],
                   table_name="silver_order_complete", severity="fail")

qc.save_results("data_quality_logs")
qc.raise_if_failed()  # stops the Gold stage if any blocking check failed
print("✅ Gold input quality gate: PASSED")

# COMMAND ----------

display(silver)

# COMMAND ----------

silver.printSchema()

# COMMAND ----------

total_revenue = silver.agg(
    sum("revenue").alias("total_revenue")
)

display(total_revenue)

# COMMAND ----------

total_orders = silver.select(
    countDistinct("order_id").alias("total_orders")
)

display(total_orders)

# COMMAND ----------

total_customers = silver.select(
    countDistinct("customer_id").alias("customers")
)

display(total_customers)

# COMMAND ----------

average_order = silver.agg(
    avg("revenue").alias("avg_order_value")
)

display(average_order)

# COMMAND ----------

daily_sales = (
    silver
    .groupBy("order_date")
    .agg(
        sum("revenue").alias("daily_revenue"),
        countDistinct("order_id").alias("orders")
    )
)

# COMMAND ----------

display(daily_sales)

# COMMAND ----------

daily_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_daily_sales")

# COMMAND ----------

monthly_sales = (
    silver
    .withColumn(
        "month",
        date_format("order_date","yyyy-MM")
    )
    .groupBy("month")
    .agg(
        sum("revenue").alias("monthly_revenue")
    )
)

# COMMAND ----------

display(monthly_sales)

# COMMAND ----------

monthly_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_monthly_sales")

# COMMAND ----------

top_customers = (
    silver
    .groupBy(
        "customer_id",
    )
    .agg(
        sum("revenue").alias("total_spent")
    )
    .orderBy(
        desc("total_spent")
    )
)

# COMMAND ----------

display(
    top_customers
)

# COMMAND ----------

top_customers.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_top_customers")

# COMMAND ----------

city_sales = (
    silver
    .groupBy("customer_city")
    .agg(
        sum("revenue").alias("city_revenue")
    )
    .orderBy(desc("city_revenue"))
)

# COMMAND ----------

display(city_sales)

# COMMAND ----------

city_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_city_sales")

# COMMAND ----------

product_sales = (
    silver
    .groupBy(
        "product_id",
    )
    .agg(
        sum("quantity").alias("quantity_sold"),
        sum("revenue").alias("revenue")
    )
)

# COMMAND ----------

display(product_sales)

# COMMAND ----------

product_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_product_sales")

# COMMAND ----------

category_sales = (
    silver
    .groupBy("product_category_name")
    .agg(
        sum("revenue").alias("revenue")
    )
    .orderBy(desc("revenue"))
)

# COMMAND ----------

display(category_sales)

# COMMAND ----------

category_sales.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_category_sales")

# COMMAND ----------

top_products = (
    product_sales
    .orderBy(
        desc("quantity_sold")
    )
)

# COMMAND ----------

display(
    top_products.limit(20)
)

# COMMAND ----------

summary = silver.agg(
    countDistinct("order_id").alias("orders"),
    countDistinct("customer_id").alias("customers"),
    sum("revenue").alias("revenue"),
    avg("revenue").alias("average_order")
)

# COMMAND ----------

display(summary)

# COMMAND ----------

summary.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("gold_dashboard_summary")

# COMMAND ----------

spark.sql("""SHOW TABLES""").show()

# COMMAND ----------

display(
    spark.table("gold_daily_sales")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_daily_sales
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_product_sales

# COMMAND ----------

# DBTITLE 1,Gold Output Quality Gate
# ── Data Quality Gate: Gold outputs ─────────────────────────────────────
# Verifies every gold table has its key columns, a unique grain, and
# nonnegative financial metrics. A blocking failure stops the stage.
# (gold_dashboard_summary is a single KPI row with no grain — checked implicitly.)
import sys
sys.path.append("/Workspace/Users/swanandikamat18@gmail.com/Project/data_quality")
from quality_checks import DataQualityChecker

qc = DataQualityChecker(spark)

GOLD_CHECKS = {
    "gold_daily_sales": (["order_date"], "daily_revenue"),
    "gold_monthly_sales": (["month"], "monthly_revenue"),
    "gold_product_sales": (["product_id"], "revenue"),
    "gold_category_sales": (["product_category_name"], "revenue"),
    "gold_city_sales": (["customer_city"], "city_revenue"),
    "gold_top_customers": (["customer_id"], "total_spent"),
}

for table, (keys, metric) in GOLD_CHECKS.items():
    df = spark.table(table)
    qc.check_required_columns(df, keys + [metric], table_name=table, severity="fail")
    qc.check_null_keys(df, keys, table_name=table, severity="fail")
    qc.check_duplicates(df, keys, table_name=table, severity="fail")
    qc.check_value_range(df, metric, min_value=0, table_name=table, severity="fail")

qc.save_results("data_quality_logs")
qc.raise_if_failed()  # stops the Gold stage if any blocking check failed
print("✅ Gold output quality gate: PASSED")