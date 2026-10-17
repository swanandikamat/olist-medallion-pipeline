# Explicit schemas for the Olist e-commerce CSV sources.
#
# These schemas make Bronze ingestion deterministic and rerun-safe: with an
# explicit schema, the same CSV file always produces the same table schema,
# unlike inferSchema whose output can vary with the file's contents.
#
# IMPORTANT NOTES:
# - Column order must match the CSV header order exactly (Spark applies a
#   StructType positionally when reading CSV files).
# - All fields are nullable=True: empty CSV values parse to NULL (same
#   behavior as the previous inferSchema reads).
# - review_score / review_creation_date / review_answer_timestamp are
#   deliberately STRING, matching the existing bronze_review table: the
#   reviews CSV has ~8.8k misaligned rows (unescaped commas/newlines in
#   comment fields), so those columns genuinely contain mixed content.
#   Bronze preserves this raw; typing/casting belongs in Silver and was NOT
#   changed here, to avoid silently altering the bronze_review schema.

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)

OLIST_SCHEMAS = {
    'orders': StructType([
        StructField('order_id', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('order_status', StringType(), True),
        StructField('order_purchase_timestamp', TimestampType(), True),
        StructField('order_approved_at', TimestampType(), True),
        StructField('order_delivered_carrier_date', TimestampType(), True),
        StructField('order_delivered_customer_date', TimestampType(), True),
        StructField('order_estimated_delivery_date', TimestampType(), True),
    ]),
    'customers': StructType([
        StructField('customer_id', StringType(), True),
        StructField('customer_unique_id', StringType(), True),
        StructField('customer_zip_code_prefix', IntegerType(), True),
        StructField('customer_city', StringType(), True),
        StructField('customer_state', StringType(), True),
    ]),
    'products': StructType([
        StructField('product_id', StringType(), True),
        StructField('product_category_name', StringType(), True),
        StructField('product_name_lenght', IntegerType(), True),
        StructField('product_description_lenght', IntegerType(), True),
        StructField('product_photos_qty', IntegerType(), True),
        StructField('product_weight_g', IntegerType(), True),
        StructField('product_length_cm', IntegerType(), True),
        StructField('product_height_cm', IntegerType(), True),
        StructField('product_width_cm', IntegerType(), True),
    ]),
    'payments': StructType([
        StructField('order_id', StringType(), True),
        StructField('payment_sequential', IntegerType(), True),
        StructField('payment_type', StringType(), True),
        StructField('payment_installments', IntegerType(), True),
        StructField('payment_value', DoubleType(), True),
    ]),
    'reviews': StructType([
        StructField('review_id', StringType(), True),
        StructField('order_id', StringType(), True),
        # STRING on purpose - see header comment (misaligned CSV rows).
        StructField('review_score', StringType(), True),
        StructField('review_comment_title', StringType(), True),
        StructField('review_comment_message', StringType(), True),
        StructField('review_creation_date', StringType(), True),
        StructField('review_answer_timestamp', StringType(), True),
    ]),
    'order_items': StructType([
        StructField('order_id', StringType(), True),
        StructField('order_item_id', IntegerType(), True),
        StructField('product_id', StringType(), True),
        StructField('seller_id', StringType(), True),
        StructField('shipping_limit_date', TimestampType(), True),
        StructField('price', DoubleType(), True),
        StructField('freight_value', DoubleType(), True),
    ]),
}
