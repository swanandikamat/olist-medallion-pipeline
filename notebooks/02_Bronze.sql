-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_orders_dataset.csv"
-- MAGIC customers_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_customers_dataset.csv"
-- MAGIC products_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_products_dataset.csv"
-- MAGIC payments_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_order_payments_dataset.csv"
-- MAGIC reviews_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_order_reviews_dataset.csv"
-- MAGIC order_items_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_order_items_dataset.csv"

-- COMMAND ----------

-- DBTITLE 1,Load CSV helper (shared + explicit schemas)
-- MAGIC %python
-- MAGIC # Shared loader + explicit schemas — replaces the inline load_csv duplicate.
-- MAGIC # Explicit schemas make reruns deterministic: same file -> same table schema
-- MAGIC # every time (inferSchema could vary if a file's contents changed).
-- MAGIC import sys
-- MAGIC sys.path.append("/Workspace/Users/swanandikamat18@gmail.com/Project/utils")
-- MAGIC sys.path.append("/Workspace/Users/swanandikamat18@gmail.com/Project/config")
-- MAGIC from common_functions import load_csv
-- MAGIC from table_schemas import OLIST_SCHEMAS

-- COMMAND ----------

-- DBTITLE 1,Load Data (explicit schemas)
-- MAGIC %python
-- MAGIC # Each load uses its explicit schema from config/table_schemas.py —
-- MAGIC # same column names, types, and order as the current bronze tables.
-- MAGIC orders = load_csv(spark, orders_path, OLIST_SCHEMAS["orders"])
-- MAGIC customers = load_csv(spark, customers_path, OLIST_SCHEMAS["customers"])
-- MAGIC products = load_csv(spark, products_path, OLIST_SCHEMAS["products"])
-- MAGIC payments = load_csv(spark, payments_path, OLIST_SCHEMAS["payments"])
-- MAGIC reviews = load_csv(spark, reviews_path, OLIST_SCHEMAS["reviews"])
-- MAGIC order_items = load_csv(spark, order_items_path, OLIST_SCHEMAS["order_items"])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp, col, lit
-- MAGIC
-- MAGIC def add_metadata(df, data_source):
-- MAGIC     return (
-- MAGIC         df.withColumn("ingestion_time", current_timestamp())
-- MAGIC           .withColumn("source_file", col("_metadata.file_path"))
-- MAGIC           .withColumn("data_source", lit(data_source))
-- MAGIC     )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_bronze = add_metadata(orders, "orders.csv")
-- MAGIC customers_bronze = add_metadata(customers, "customers.csv")
-- MAGIC products_bronze = add_metadata(products, "products.csv")
-- MAGIC payments_bronze = add_metadata(payments, "payments.csv")
-- MAGIC reviews_bronze = add_metadata(reviews, "reviews.csv")
-- MAGIC order_items_bronze = add_metadata(order_items, "order_items.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_bronze.show(1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     orders_bronze.write
-- MAGIC         .format("delta")
-- MAGIC         .mode("overwrite")
-- MAGIC         .saveAsTable("bronze_orders")
-- MAGIC )
-- MAGIC        
-- MAGIC (
-- MAGIC     customers_bronze.write
-- MAGIC         .format("delta")
-- MAGIC         .mode("overwrite")
-- MAGIC         .saveAsTable("bronze_customers")
-- MAGIC )
-- MAGIC        
-- MAGIC (
-- MAGIC     products_bronze.write
-- MAGIC         .format("delta")
-- MAGIC         .mode("overwrite")
-- MAGIC         .saveAsTable("bronze_products")
-- MAGIC )
-- MAGIC        
-- MAGIC (
-- MAGIC     payments_bronze.write
-- MAGIC         .format("delta")
-- MAGIC         .mode("overwrite")
-- MAGIC         .saveAsTable("bronze_payments")
-- MAGIC )
-- MAGIC
-- MAGIC (
-- MAGIC     reviews_bronze.write
-- MAGIC         .format("delta")
-- MAGIC         .mode("overwrite")
-- MAGIC         .saveAsTable("bronze_review")
-- MAGIC )
-- MAGIC
-- MAGIC (
-- MAGIC     order_items_bronze.write
-- MAGIC         .format("delta")
-- MAGIC         .mode("overwrite")
-- MAGIC         .saveAsTable("bronze_order_items")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("SHOW TABLES").show(truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC bronze_orders = spark.table("bronze_orders")

-- COMMAND ----------

DESCRIBE HISTORY bronze_orders;

-- COMMAND ----------

-- DBTITLE 1,Bronze Data Quality Gate
-- MAGIC %python
-- MAGIC # ── Data Quality Gate: Bronze ─────────────────────────────────────────
-- MAGIC # Blocking checks (severity="fail") stop this stage with a clear error.
-- MAGIC # Advisory checks (severity="warn") are logged to data_quality_logs but never stop the run.
-- MAGIC import sys
-- MAGIC sys.path.append("/Workspace/Users/swanandikamat18@gmail.com/Project/data_quality")
-- MAGIC from quality_checks import DataQualityChecker
-- MAGIC
-- MAGIC qc = DataQualityChecker(spark)
-- MAGIC
-- MAGIC REQUIRED_COLUMNS = {
-- MAGIC     "bronze_orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
-- MAGIC     "bronze_customers": ["customer_id", "customer_unique_id", "customer_city"],
-- MAGIC     "bronze_payments": ["order_id", "payment_sequential", "payment_value"],
-- MAGIC     "bronze_order_items": ["order_id", "order_item_id", "product_id", "price", "freight_value"],
-- MAGIC }
-- MAGIC
-- MAGIC for table, required in REQUIRED_COLUMNS.items():
-- MAGIC     df = spark.table(table)
-- MAGIC     qc.check_required_columns(df, required, table_name=table, severity="fail")
-- MAGIC     qc.check_null_keys(df, [required[0]], table_name=table, severity="fail")
-- MAGIC
-- MAGIC # Known data fact (verified in the Phase 1 audit): bronze_review contains
-- MAGIC # duplicate review_id values. Reviews are not used downstream yet, so this
-- MAGIC # is a WARNING, not a blocking failure.
-- MAGIC qc.check_duplicates(spark.table("bronze_review"), ["review_id"],
-- MAGIC                     table_name="bronze_review", severity="warn")
-- MAGIC
-- MAGIC qc.save_results("data_quality_logs")
-- MAGIC qc.raise_if_failed()  # stops the Bronze stage if any blocking check failed
-- MAGIC print("✅ Bronze data quality gate: PASSED")