# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

orders = spark.table("bronze_orders")

customers = spark.table("bronze_customers")

products = spark.table("bronze_products")

payments = spark.table("bronze_payments")

reviews = spark.table("bronze_review")

order_items = spark.table("bronze_order_items")

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

print("Orders:", orders.count())

# COMMAND ----------

orders.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in orders.columns
]).show()

# COMMAND ----------

orders = orders.dropDuplicates()

# COMMAND ----------

display(
    orders.groupBy(orders.columns)
    .count()
    .filter(col("count") > 1)
)

# COMMAND ----------

customers.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in customers.columns
]).show()

# COMMAND ----------

products.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in products.columns
]).show()

# COMMAND ----------

products.show(5)

# COMMAND ----------

products = products.dropDuplicates()

# COMMAND ----------

payments.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in payments.columns
]).show()

# COMMAND ----------

reviews.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in reviews.columns
]).show()

# COMMAND ----------

reviews = reviews.dropDuplicates(['review_id'])

# COMMAND ----------

reviews = reviews.dropDuplicates()

# COMMAND ----------

order_customer = orders.join(
    customers,
    on="customer_id",
    how="left"
)

# COMMAND ----------

silver = order_customer.join(
    products,
    on="product_id",
    how="left"
)

# COMMAND ----------

orders.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("silver_orders")




# COMMAND ----------

customers.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("silver_customers")




# COMMAND ----------

products.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("silver_products")


# COMMAND ----------

orders_with_revenue = orders.join(
    payments,
    on="order_id",
    how="left"
)

# COMMAND ----------

revenue_per_order = orders_with_revenue.groupBy("order_id").agg(
    sum("payment_value").alias("total_revenue"),
    count("payment_sequential").alias("payment_count")
)
revenue_per_order.show(10)

# COMMAND ----------

payments = payments.filter(
    col("payment_value") > 0
)

# COMMAND ----------

# DBTITLE 1,Create silver_final with quantity and revenue
# First, calculate quantity for each product per order
order_items_with_qty = order_items.groupBy("order_id", "product_id").agg(
    count("*").alias("quantity"),
    first("order_item_id").alias("order_item_id"),
    first("seller_id").alias("seller_id"),
    first("price").alias("price"),
    sum("freight_value").alias("freight_value")
)

# Calculate revenue = price * quantity
order_items_with_qty = order_items_with_qty.withColumn(
    "revenue", 
    col("price") * col("quantity")
)

silver_final = (
    orders
    .join(order_items_with_qty, on="order_id", how="left")  
    .join(products, on="product_id", how="left")  
    .join(customers, on="customer_id", how="left") 
    .join(
        payments.groupBy("order_id").agg(
            sum("payment_value").alias("total_revenue"),
            count("payment_sequential").alias("payment_count"),
            collect_list("payment_type").alias("payment_types")
        ),
        on="order_id",
        how="left"
    ) 
)

# COMMAND ----------

silver_final = silver_final.select(
    # Order identification & timing
    "order_id",
    to_date("order_purchase_timestamp").alias("order_date"),  # Convert timestamp to date
    "order_status",  # Essential: filter delivered vs cancelled
    
    # Customer information
    "customer_id",
    "customer_city",
    "customer_state",
    
    # Product & seller
    "product_id",
    "product_category_name",  # Product type
    "seller_id",
    
    # Financial metrics
    "price",           # Item price
    "quantity",        # Items ordered
    "revenue",         # Item revenue (price × quantity)
    "freight_value",   # Shipping cost
    
    # Order-level aggregates
    "total_revenue"    # Total payment for entire order
)

# COMMAND ----------

# DBTITLE 1,Drop rows with null values
# Drop rows with any null values
silver_final = silver_final.dropna()

# COMMAND ----------

silver_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_order_complete")

# COMMAND ----------

# DBTITLE 1,Silver Data Quality Gate
# ── Data Quality Gate: Silver ──────────────────────────────────────────
# Validates silver_order_complete at its grain: one row per (order_id, product_id).
# A blocking failure stops the Silver stage BEFORE the table is rebuilt downstream.
import sys
sys.path.append("/Workspace/Users/swanandikamat18@gmail.com/Project/data_quality")
from quality_checks import DataQualityChecker

qc = DataQualityChecker(spark)
silver_tbl = spark.table("silver_order_complete")

qc.check_required_columns(
    silver_tbl,
    ["order_id", "order_date", "order_status", "customer_id", "product_id",
     "price", "quantity", "revenue", "freight_value", "total_revenue"],
    table_name="silver_order_complete", severity="fail")

qc.check_null_keys(silver_tbl, ["order_id", "product_id"],
                   table_name="silver_order_complete", severity="fail")

qc.check_duplicates(silver_tbl, ["order_id", "product_id"],
                     table_name="silver_order_complete", severity="fail")

qc.check_valid_dates(silver_tbl, ["order_date"],
                     min_date="2016-01-01", max_date="2018-12-31",
                     table_name="silver_order_complete", severity="fail")

qc.check_value_range(silver_tbl, "price", min_value=0,
                     table_name="silver_order_complete", severity="fail")
qc.check_value_range(silver_tbl, "quantity", min_value=1,
                     table_name="silver_order_complete", severity="fail")
qc.check_value_range(silver_tbl, "revenue", min_value=0,
                     table_name="silver_order_complete", severity="fail")

# Every order in silver must still exist in bronze (no invented keys)
qc.check_referential_integrity(
    spark.table("bronze_orders"), silver_tbl, "order_id", "order_id",
    table_name="silver_order_complete", severity="fail")

qc.save_results("data_quality_logs")
qc.raise_if_failed()  # stops the Silver stage if any blocking check failed
print("✅ Silver data quality gate: PASSED")

# COMMAND ----------

display(silver_final.limit(10))

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_order_complete
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total revenue across all orders
# MAGIC SELECT
# MAGIC   SUM(revenue) AS total_revenue,
# MAGIC   COUNT(DISTINCT order_id) AS total_orders,
# MAGIC   AVG(revenue) AS avg_revenue_per_item
# MAGIC FROM silver_order_complete;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC customer_city,
# MAGIC COUNT(*) orders
# MAGIC FROM silver_order_complete
# MAGIC GROUP BY customer_city
# MAGIC ORDER BY orders DESC;