"""Common utility functions for the e-commerce data pipeline.

This module contains reusable functions used across multiple notebooks.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, sum, count, current_timestamp, lit, when, 
    year, month, dayofmonth, date_format
)
from pyspark.sql.types import StructType
from functools import wraps
import time
import yaml


def load_config(config_path: str = "/Workspace/Users/swanandikamat18@gmail.com/Project/config/pipeline_config.yaml") -> dict:
    """Load pipeline configuration from YAML file.
    
    Args:
        config_path: Path to configuration YAML file
        
    Returns:
        Configuration dictionary
    """
    # NOTE: /dbfs paths are NOT accessible on serverless compute (Free Edition).
    # The /Workspace path opens directly from Python — no rewrite needed.
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def add_audit_columns(df: DataFrame) -> DataFrame:
    """Add standard audit columns to a DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with audit columns added
    """
    return df \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("ingestion_date", current_timestamp().cast("date")) \
        .withColumn("source_system", lit("olist_ecommerce"))


def validate_dataframe(df: DataFrame, name: str, min_rows: int = 1000) -> dict:
    """Validate DataFrame and return quality metrics.
    
    Args:
        df: DataFrame to validate
        name: Name of the dataset for logging
        min_rows: Minimum expected row count
        
    Returns:
        Dictionary with validation results
    """
    print("=" * 60)
    print(f"Validating Dataset: {name}")
    print("=" * 60)
    
    row_count = df.count()
    col_count = len(df.columns)
    
    # Null checks
    null_counts = df.select(
        [sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
    ).collect()[0].asDict()
    
    # Duplicate check
    duplicate_count = row_count - df.dropDuplicates().count()
    
    # Validation results
    results = {
        "dataset_name": name,
        "row_count": row_count,
        "column_count": col_count,
        "null_counts": null_counts,
        "duplicate_count": duplicate_count,
        "validation_passed": row_count >= min_rows and duplicate_count == 0
    }
    
    print(f"Rows: {row_count}")
    print(f"Columns: {col_count}")
    
    print("\nSchema")
    df.printSchema()
    
    print("\nFirst Five Rows")
    df.show(5)
    
    print("\nNull Values")
    for c in df.columns:
        print(f"  {c}: {null_counts[c]}")
    
    print("\nDuplicates")
    print(duplicate_count)
    print(f"Validation Passed: {results['validation_passed']}")
    print("=" * 60)
    
    return results


def retry_on_failure(max_retries: int = 3, delay: int = 5):
    """Decorator to retry functions on failure.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Delay between retries in seconds
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        print(f"Function {func.__name__} failed after {max_retries} attempts")
                        raise
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
        return wrapper
    return decorator


def read_with_schema(spark: SparkSession, path: str, format: str = "csv", **options) -> DataFrame:
    """Read data with explicit error handling.
    
    Args:
        spark: SparkSession
        path: Data source path
        format: File format (csv, parquet, delta, etc.)
        **options: Additional read options
        
    Returns:
        DataFrame
    """
    try:
        df = spark.read.format(format).options(**options).load(path)
        print(f"Successfully loaded {df.count()} rows from {path}")
        return df
    except Exception as e:
        print(f"Error reading from {path}: {e}")
        raise


def load_csv(spark: SparkSession, path: str, schema: StructType = None) -> DataFrame:
    """Read a CSV file with a header row, optionally with an explicit schema.

    Shared loader used by the ingestion and Bronze notebooks (replaces the
    duplicated inline reader definitions).

    With an explicit schema (recommended for Bronze), the same file always
    produces the same DataFrame schema — reruns are deterministic. Without
    one, inferSchema is used (fine for exploration, but the inferred schema
    can vary with the file's contents, e.g. a misaligned CSV column).

    Args:
        spark: Active SparkSession
        path: CSV file path
        schema: Optional explicit StructType (column order must match the CSV)

    Returns:
        DataFrame
    """
    reader = spark.read.format("csv").option("header", True)
    if schema is not None:
        reader = reader.option("timestampFormat", "yyyy-MM-dd HH:mm:ss").schema(schema)
    else:
        reader = reader.option("inferSchema", True)
    return reader.load(path)


def write_delta_table(spark: SparkSession, df: DataFrame, table_name: str,
                      mode: str = "overwrite", partition_by: list = None) -> None:
    """Write a DataFrame to a Delta table (default: full overwrite).

    Overwrite is the right mode for the static Olist dataset: reruns are
    idempotent (same CSVs -> identical table contents, as a new Delta version;
    previous versions remain available via time travel). Do NOT use append
    for full-file reloads — it duplicates every row on each rerun. Do NOT use
    merge/upsert unless the source has a reliable change key or timestamp
    (the Olist CSVs are static snapshots with no change marker, so merge is
    not applicable here).

    Args:
        spark: Active SparkSession (explicit — this module has no global spark)
        df: DataFrame to write
        table_name: Table name (e.g. "bronze_orders")
        mode: Write mode (default "overwrite")
        partition_by: Optional list of partition columns
    """
    writer = df.write.format("delta").mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.saveAsTable(table_name)
    print(f"Successfully wrote {df.count()} rows to {table_name}")


def log_pipeline_metrics(stage: str, metrics: dict, spark: SparkSession) -> None:
    """Log pipeline execution metrics to monitoring table.
    
    Args:
        stage: Pipeline stage name (bronze, silver, gold)
        metrics: Dictionary of metrics to log
        spark: SparkSession
    """
    from datetime import datetime
    
    metrics_df = spark.createDataFrame([
        {
            "pipeline_stage": stage,
            "execution_time": datetime.now(),
            "row_count": metrics.get("row_count", 0),
            "status": metrics.get("status", "success"),
            "error_message": metrics.get("error", None)
        }
    ])
    
    metrics_df.write.format("delta").mode("append").saveAsTable("pipeline_metrics_log")