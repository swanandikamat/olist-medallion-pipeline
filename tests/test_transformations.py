"""Unit tests for pipeline transformations.

Run these tests before deploying to production.
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime

# Import the project's data quality framework (works on serverless compute)
sys.path.append("/Workspace/Users/swanandikamat18@gmail.com/Project/data_quality")
from quality_checks import DataQualityChecker

# Initialize Spark session for testing
spark = SparkSession.builder.appName("PipelineTests").getOrCreate()


def test_revenue_calculation():
    """Test revenue calculation logic."""
    print("Testing: Revenue Calculation")
    
    # Create test data
    test_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("quantity", IntegerType(), False)
    ])
    
    test_data = [
        ("order_1", 10.0, 2),
        ("order_2", 20.5, 3),
        ("order_3", 15.0, 1)
    ]
    
    df = spark.createDataFrame(test_data, test_schema)
    
    # Apply transformation
    result_df = df.withColumn("revenue", col("price") * col("quantity"))
    
    # Validate results
    results = result_df.collect()
    
    assert results[0]["revenue"] == 20.0, "Revenue calculation failed for order_1"
    assert results[1]["revenue"] == 61.5, "Revenue calculation failed for order_2"
    assert results[2]["revenue"] == 15.0, "Revenue calculation failed for order_3"
    
    print("✅ PASSED: Revenue calculation test")
    return True


def test_null_filtering():
    """Test null value filtering."""
    print("Testing: Null Value Filtering")
    
    test_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])
    
    test_data = [
        ("order_1", "cust_1", 100.0),
        ("order_2", None, 200.0),  # Should be filtered
        ("order_3", "cust_3", None),  # Should be filtered
        ("order_4", "cust_4", 300.0)
    ]
    
    df = spark.createDataFrame(test_data, test_schema)
    
    # Apply null filtering
    clean_df = df.dropna()
    
    # Validate
    assert clean_df.count() == 2, "Null filtering did not work correctly"
    assert clean_df.filter(col("order_id") == "order_2").count() == 0
    assert clean_df.filter(col("order_id") == "order_3").count() == 0
    
    print("✅ PASSED: Null filtering test")
    return True


def test_date_conversion():
    """Test timestamp to date conversion."""
    print("Testing: Date Conversion")
    
    test_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("order_timestamp", TimestampType(), False)
    ])
    
    test_data = [
        ("order_1", datetime(2023, 1, 15, 14, 30, 0)),
        ("order_2", datetime(2023, 2, 20, 9, 15, 0))
    ]
    
    df = spark.createDataFrame(test_data, test_schema)
    
    # Apply transformation
    result_df = df.withColumn("order_date", to_date(col("order_timestamp")))
    
    # Validate
    results = result_df.collect()
    assert str(results[0]["order_date"]) == "2023-01-15"
    assert str(results[1]["order_date"]) == "2023-02-20"
    
    print("✅ PASSED: Date conversion test")
    return True


def test_aggregation_logic():
    """Test aggregation calculations."""
    print("Testing: Aggregation Logic")
    
    test_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("sales", DoubleType(), False)
    ])
    
    test_data = [
        ("prod_1", "electronics", 100.0),
        ("prod_2", "electronics", 150.0),
        ("prod_3", "books", 50.0),
        ("prod_4", "books", 75.0)
    ]
    
    df = spark.createDataFrame(test_data, test_schema)
    
    # Apply aggregation
    agg_df = df.groupBy("category").agg(
        sum("sales").alias("total_sales"),
        count("product_id").alias("product_count"),
        avg("sales").alias("avg_sales")
    )
    
    # Validate
    electronics = agg_df.filter(col("category") == "electronics").collect()[0]
    books = agg_df.filter(col("category") == "books").collect()[0]
    
    assert electronics["total_sales"] == 250.0
    assert electronics["product_count"] == 2
    assert books["total_sales"] == 125.0
    assert books["product_count"] == 2
    
    print("✅ PASSED: Aggregation logic test")
    return True


def test_join_logic():
    """Test DataFrame join operations."""
    print("Testing: Join Logic")
    
    # Create orders DataFrame
    orders_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount", DoubleType(), False)
    ])
    
    orders_data = [
        ("order_1", "cust_1", 100.0),
        ("order_2", "cust_2", 200.0)
    ]
    
    # Create customers DataFrame
    customers_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False)
    ])
    
    customers_data = [
        ("cust_1", "John Doe"),
        ("cust_2", "Jane Smith")
    ]
    
    orders_df = spark.createDataFrame(orders_data, orders_schema)
    customers_df = spark.createDataFrame(customers_data, customers_schema)
    
    # Apply join
    joined_df = orders_df.join(customers_df, "customer_id", "inner")
    
    # Validate
    assert joined_df.count() == 2, "Join did not produce expected row count"
    assert "customer_name" in joined_df.columns, "Customer name column missing after join"
    
    print("✅ PASSED: Join logic test")
    return True


def test_required_columns_check():
    """DQ framework: missing required columns are detected."""
    print("Testing: Required Columns Check")
    qc = DataQualityChecker(spark)
    df = spark.createDataFrame([("order_1", 10.0)], ["order_id", "price"])

    passed_bad, bad = qc.check_required_columns(df, ["order_id", "price", "customer_id"])
    assert passed_bad is False, "Missing column was not detected"
    assert bad["missing_columns"] == ["customer_id"], "Wrong missing column reported"

    passed_ok, _ = qc.check_required_columns(df, ["order_id", "price"])
    assert passed_ok is True, "Complete schema incorrectly flagged"

    print("✅ PASSED: Required columns check test")
    return True


def test_null_primary_keys_check():
    """DQ framework: NULLs in grain-defining key columns are detected."""
    print("Testing: Null Primary Keys Check")
    qc = DataQualityChecker(spark)
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True)
    ])
    df = spark.createDataFrame(
        [("order_1", "prod_1"), (None, "prod_2"), ("order_3", None)], schema)

    passed, results = qc.check_null_keys(df, ["order_id", "product_id"])
    assert passed is False, "NULL keys were not detected"
    assert results["null_counts"]["order_id"] == 1, "Wrong NULL count for order_id"
    assert results["null_counts"]["product_id"] == 1, "Wrong NULL count for product_id"

    print("✅ PASSED: Null primary keys check test")
    return True


def test_duplicate_keys_check():
    """DQ framework: duplicate keys at the table grain are detected."""
    print("Testing: Duplicate Keys Check")
    qc = DataQualityChecker(spark)
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), False)
    ])
    df = spark.createDataFrame(
        [("order_1", "prod_1"), ("order_1", "prod_1"), ("order_2", "prod_2")], schema)

    passed, results = qc.check_duplicates(df, ["order_id", "product_id"])
    assert passed is False, "Duplicate keys were not detected"
    assert results["duplicate_count"] == 1, "Wrong duplicate count reported"

    print("✅ PASSED: Duplicate keys check test")
    return True


def test_valid_dates_check():
    """DQ framework: NULL and out-of-range dates are detected."""
    print("Testing: Valid Dates Check")
    qc = DataQualityChecker(spark)
    df = spark.createDataFrame(
        [("o1", "2023-01-15"), ("o2", "2024-05-01"), ("o3", None)],
        ["order_id", "d"]
    ).withColumn("order_date", to_date(col("d")))

    passed, results = qc.check_valid_dates(
        df, ["order_date"], min_date="2023-01-01", max_date="2023-12-31")
    assert passed is False, "Out-of-range and NULL dates were not detected"
    assert results["invalid_counts"]["order_date"] == 2, "Wrong invalid date count"

    print("✅ PASSED: Valid dates check test")
    return True


def test_nonnegative_values_check():
    """DQ framework: negative prices are detected by the range check."""
    print("Testing: Nonnegative Values Check")
    qc = DataQualityChecker(spark)
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("price", DoubleType(), False)
    ])
    df = spark.createDataFrame([("order_1", -5.0), ("order_2", 10.0)], schema)

    passed, results = qc.check_value_range(df, "price", min_value=0)
    assert passed is False, "Negative price was not detected"
    assert results["out_of_range_count"] == 1, "Wrong out-of-range count"

    print("✅ PASSED: Nonnegative values check test")
    return True


def test_referential_integrity_check():
    """DQ framework: orphan foreign keys are detected."""
    print("Testing: Referential Integrity Check")
    qc = DataQualityChecker(spark)
    orders_df = spark.createDataFrame(
        [("order_1", "cust_1"), ("order_2", "cust_999")],
        ["order_id", "customer_id"])
    customers_df = spark.createDataFrame(
        [("cust_1", "John"), ("cust_2", "Jane")],
        ["customer_id", "name"])

    passed, results = qc.check_referential_integrity(
        customers_df, orders_df, "customer_id", "customer_id")
    assert passed is False, "Orphan customer_id was not detected"
    assert results["orphan_records"] == 1, "Wrong orphan count"

    print("✅ PASSED: Referential integrity check test")
    return True


def test_financial_reconciliation_check():
    """DQ framework: payment totals reconcile to item price + freight."""
    print("Testing: Financial Reconciliation Check")
    qc = DataQualityChecker(spark)

    items_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("freight_value", DoubleType(), False)
    ])
    items_df = spark.createDataFrame(
        [("o1", 10.0, 2.0), ("o2", 20.0, 5.0)], items_schema)
    payments_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("payment_value", DoubleType(), False)
    ])
    # o2 pays 30.0 but its items + freight = 25.0 -> one mismatching order
    payments_df = spark.createDataFrame(
        [("o1", 12.0), ("o2", 30.0)], payments_schema)

    passed_bad, bad = qc.check_financial_reconciliation(
        items_df, payments_df, max_mismatch_pct=1.0)
    assert passed_bad is False, "50% mismatch should exceed a 1% tolerance"
    assert bad["orders_mismatched"] == 1, "Wrong mismatch count"

    passed_ok, ok = qc.check_financial_reconciliation(
        items_df, payments_df, max_mismatch_pct=60.0)
    assert passed_ok is True, "50% mismatch should be within a 60% tolerance"

    print("✅ PASSED: Financial reconciliation check test")
    return True


def test_warning_vs_failure_gate():
    """DQ framework: warnings never stop the pipeline; failures do."""
    print("Testing: Warning vs Failure Gate")
    qc = DataQualityChecker(spark)

    # A failed WARNING must not raise
    qc.log_results("bronze_review", "check_duplicates", False,
                   {"duplicate_count": 1}, severity="warn")
    qc.raise_if_failed()  # no exception expected

    # A failed BLOCKING check must raise a clear RuntimeError
    qc.log_results("silver_order_complete", "check_null_keys", False,
                   {"total_nulls": 1}, severity="fail")
    raised = False
    try:
        qc.raise_if_failed()
    except RuntimeError:
        raised = True
    assert raised, "raise_if_failed() did not raise on a blocking failure"

    print("✅ PASSED: Warning vs failure gate test")
    return True


def run_all_tests():
    """Run all test cases."""
    print("="*60)
    print("Running Pipeline Test Suite")
    print("="*60)
    
    tests = [
        test_revenue_calculation,
        test_null_filtering,
        test_date_conversion,
        test_aggregation_logic,
        test_join_logic,
        test_required_columns_check,
        test_null_primary_keys_check,
        test_duplicate_keys_check,
        test_valid_dates_check,
        test_nonnegative_values_check,
        test_referential_integrity_check,
        test_financial_reconciliation_check,
        test_warning_vs_failure_gate
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"❌ FAILED: {test.__name__} - {e}")
            failed += 1
    
    print("="*60)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("="*60)
    
    return failed == 0


# Run tests when executed
if __name__ == "__main__":
    success = run_all_tests()
    if not success:
        raise Exception("Some tests failed!")