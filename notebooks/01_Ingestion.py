# Databricks notebook source
# DBTITLE 1,Imports + shared helpers
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Shared helpers (replaces the inline load/validate definitions below)
import sys
sys.path.append("/Workspace/Users/swanandikamat18@gmail.com/Project/utils")
from common_functions import load_csv, validate_dataframe

# COMMAND ----------

# DBTITLE 1,Load orders (shared loader)
# Shared loader from utils/common_functions.py (replaces the inline read)
orders = load_csv(
    spark,
    "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_orders_dataset.csv"
)

orders.show()

# COMMAND ----------

orders.show(10)

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

len(orders.columns)

# COMMAND ----------

orders.columns

# COMMAND ----------

orders.describe().show()

# COMMAND ----------

orders.select(
    [
        sum(col(c).isNull().cast("int")).alias(c)
        for c in orders.columns
    ]
).show()

# COMMAND ----------

duplicate_rows = (
    orders.count()
    -
    orders.dropDuplicates().count()
)

print("Duplicate Rows :", duplicate_rows)

# COMMAND ----------

orders.select("order_status").distinct().show()

# COMMAND ----------

orders.dtypes

# COMMAND ----------

orders.explain(True)

# COMMAND ----------

orders_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_orders_dataset.csv"
customers_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_customers_dataset.csv"
products_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_products_dataset.csv"
payments_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_order_payments_dataset.csv"
reviews_path = "/Workspace/Users/swanandikamat18@gmail.com/Project/Data/olist_order_reviews_dataset.csv"

# COMMAND ----------

# DBTITLE 1,Load datasets (shared loader)
# Shared loader from utils/common_functions.py (replaces 4 inline reads).
# Exploration notebook: no explicit schema here — deterministic schemas are
# enforced in 02_Bronze via config/table_schemas.py.
customers = load_csv(spark, customers_path)
products = load_csv(spark, products_path)
payments = load_csv(spark, payments_path)
reviews = load_csv(spark, reviews_path)

# COMMAND ----------

validate_dataframe(orders, "Orders")

# COMMAND ----------

validate_dataframe(customers, "Customers")

# COMMAND ----------

validate_dataframe(products, "Products")

# COMMAND ----------

validate_dataframe(payments, "Payments")

# COMMAND ----------

validate_dataframe(reviews, "Reviews")

# COMMAND ----------

