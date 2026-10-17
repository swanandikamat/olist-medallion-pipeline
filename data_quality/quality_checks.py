"""Data Quality Framework for E-commerce Pipeline.

This module contains comprehensive data quality checks and expectations.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, when, current_date, lit, abs, to_date
)
from typing import Dict, List, Tuple
from datetime import datetime


class DataQualityChecker:
    """Data quality validation framework."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results = []
    
    def check_null_values(self, df: DataFrame, columns: List[str], 
                         max_null_percentage: float = 5.0) -> Tuple[bool, dict]:
        """Check null value percentages in specified columns.
        
        Args:
            df: DataFrame to check
            columns: List of column names to validate
            max_null_percentage: Maximum allowed null percentage
            
        Returns:
            Tuple of (pass/fail boolean, results dict)
        """
        total_rows = df.count()
        null_checks = {}
        passed = True
        
        for col_name in columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_pct = (null_count / total_rows) * 100
            
            check_passed = null_pct <= max_null_percentage
            null_checks[col_name] = {
                "null_count": null_count,
                "null_percentage": round(null_pct, 2),
                "passed": check_passed
            }
            
            if not check_passed:
                passed = False
                print(f"❌ FAILED: {col_name} has {null_pct:.2f}% nulls (threshold: {max_null_percentage}%)")
            else:
                print(f"✅ PASSED: {col_name} has {null_pct:.2f}% nulls")
        
        return passed, null_checks
    
    def check_duplicates(self, df: DataFrame, key_columns: List[str],
                        table_name: str = None, severity: str = "fail") -> Tuple[bool, dict]:
        """Check for duplicate records based on key columns.
        
        Args:
            df: DataFrame to check
            key_columns: Columns that define uniqueness (the table grain)
            table_name: Table name used when logging the result
            severity: "fail" blocks the pipeline via raise_if_failed(); "warn" only logs
            
        Returns:
            Tuple of (pass/fail boolean, results dict)
        """
        total_rows = df.count()
        unique_rows = df.dropDuplicates(key_columns).count()
        duplicate_count = total_rows - unique_rows
        
        passed = duplicate_count == 0
        
        results = {
            "total_rows": total_rows,
            "unique_rows": unique_rows,
            "duplicate_count": duplicate_count,
            "passed": passed
        }
        
        if passed:
            print(f"✅ PASSED: No duplicates found on {key_columns}")
        elif severity == "warn":
            print(f"⚠️ WARNING: Found {duplicate_count} duplicate rows on {key_columns} (non-blocking)")
        else:
            print(f"❌ FAILED: Found {duplicate_count} duplicate rows on {key_columns}")
        
        if table_name:
            self.log_results(table_name, "check_duplicates", passed, results, severity)
        return passed, results
    
    def check_value_range(self, df: DataFrame, column: str, 
                         min_value: float = None, max_value: float = None,
                         table_name: str = None, severity: str = "fail") -> Tuple[bool, dict]:
        """Check if numeric values fall within expected range.
        
        Args:
            df: DataFrame to check
            column: Column name to validate
            min_value: Minimum expected value (e.g. 0 for prices)
            max_value: Maximum expected value
            table_name: Table name used when logging the result
            severity: "fail" blocks the pipeline via raise_if_failed(); "warn" only logs
            
        Returns:
            Tuple of (pass/fail boolean, results dict)
        """
        out_of_range = df.filter(
            (col(column) < min_value if min_value is not None else lit(False)) |
            (col(column) > max_value if max_value is not None else lit(False))
        ).count()
        
        passed = out_of_range == 0
        
        results = {
            "column": column,
            "out_of_range_count": out_of_range,
            "min_value": min_value,
            "max_value": max_value,
            "passed": passed
        }
        
        if passed:
            print(f"✅ PASSED: {column} values within range [{min_value}, {max_value}]")
        elif severity == "warn":
            print(f"⚠️ WARNING: {column} has {out_of_range} values outside range (non-blocking)")
        else:
            print(f"❌ FAILED: {column} has {out_of_range} values outside range")
        
        if table_name:
            self.log_results(table_name, "check_value_range", passed, results, severity)
        return passed, results
    
    def check_referential_integrity(self, parent_df: DataFrame, child_df: DataFrame,
                                   parent_key: str, child_key: str,
                                   table_name: str = None, severity: str = "fail") -> Tuple[bool, dict]:
        """Check referential integrity between two DataFrames.
        
        Args:
            parent_df: Parent DataFrame (e.g., customers)
            child_df: Child DataFrame (e.g., orders)
            parent_key: Primary key column in parent
            child_key: Foreign key column in child
            table_name: Table name used when logging the result
            severity: "fail" blocks the pipeline via raise_if_failed(); "warn" only logs
            
        Returns:
            Tuple of (pass/fail boolean, results dict)
        """
        orphan_records = child_df \
            .join(parent_df, child_df[child_key] == parent_df[parent_key], "left_anti") \
            .count()
        
        passed = orphan_records == 0
        
        results = {
            "parent_key": parent_key,
            "child_key": child_key,
            "orphan_records": orphan_records,
            "passed": passed
        }
        
        if passed:
            print(f"✅ PASSED: All {child_key} values exist in {parent_key}")
        elif severity == "warn":
            print(f"⚠️ WARNING: Found {orphan_records} orphan records (non-blocking)")
        else:
            print(f"❌ FAILED: Found {orphan_records} orphan records")
        
        if table_name:
            self.log_results(table_name, "check_referential_integrity", passed, results, severity)
        return passed, results
    
    def check_freshness(self, df: DataFrame, date_column: str, 
                       max_age_days: int = 1) -> Tuple[bool, dict]:
        """Check data freshness based on timestamp column.
        
        Args:
            df: DataFrame to check
            date_column: Column containing timestamp/date
            max_age_days: Maximum allowed age in days
            
        Returns:
            Tuple of (pass/fail boolean, results dict)
        """
        max_date = df.agg({date_column: "max"}).collect()[0][0]
        
        if max_date:
            from datetime import datetime, timedelta
            days_old = (datetime.now() - max_date).days
            passed = days_old <= max_age_days
        else:
            days_old = None
            passed = False
        
        results = {
            "latest_date": max_date,
            "days_old": days_old,
            "max_age_days": max_age_days,
            "passed": passed
        }
        
        if passed:
            print(f"✅ PASSED: Data is {days_old} days old (threshold: {max_age_days})")
        else:
            print(f"❌ FAILED: Data is {days_old} days old (threshold: {max_age_days})")
        
        return passed, results
    
    def check_schema_compliance(self, df: DataFrame, expected_schema: Dict[str, str]) -> Tuple[bool, dict]:
        """Validate DataFrame schema matches expectations.
        
        Args:
            df: DataFrame to validate
            expected_schema: Dict of {column_name: data_type}
            
        Returns:
            Tuple of (pass/fail boolean, results dict)
        """
        actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}
        
        missing_columns = set(expected_schema.keys()) - set(actual_schema.keys())
        extra_columns = set(actual_schema.keys()) - set(expected_schema.keys())
        
        type_mismatches = []
        for col_name in expected_schema:
            if col_name in actual_schema:
                if expected_schema[col_name] != actual_schema[col_name]:
                    type_mismatches.append({
                        "column": col_name,
                        "expected": expected_schema[col_name],
                        "actual": actual_schema[col_name]
                    })
        
        passed = len(missing_columns) == 0 and len(extra_columns) == 0 and len(type_mismatches) == 0
        
        results = {
            "missing_columns": list(missing_columns),
            "extra_columns": list(extra_columns),
            "type_mismatches": type_mismatches,
            "passed": passed
        }
        
        if passed:
            print("✅ PASSED: Schema matches expectations")
        else:
            print(f"❌ FAILED: Schema validation failed")
            if missing_columns:
                print(f"  Missing columns: {missing_columns}")
            if extra_columns:
                print(f"  Extra columns: {extra_columns}")
            if type_mismatches:
                print(f"  Type mismatches: {type_mismatches}")
        
        return passed, results
    
    def check_required_columns(self, df: DataFrame, required_columns: List[str],
                               table_name: str = None, severity: str = "fail") -> Tuple[bool, dict]:
        """Check that all required columns exist in the DataFrame.
        
        Args:
            df: DataFrame to check
            required_columns: Columns that must be present
            table_name: Table name used when logging the result
            severity: "fail" blocks the pipeline via raise_if_failed(); "warn" only logs
            
        Returns:
            Tuple of (pass/fail boolean, results dict)
        """
        missing = [c for c in required_columns if c not in df.columns]
        passed = len(missing) == 0
        
        results = {
            "required_columns": required_columns,
            "missing_columns": missing,
            "passed": passed
        }
        
        if passed:
            print(f"✅ PASSED: All required columns present")
        elif severity == "warn":
            print(f"⚠️ WARNING: Missing columns {missing} (non-blocking)")
        else:
            print(f"❌ FAILED: Missing required columns: {missing}")
        
        if table_name:
            self.log_results(table_name, "check_required_columns", passed, results, severity)
        return passed, results
    
    def check_null_keys(self, df: DataFrame, key_columns: List[str],
                        table_name: str = None, severity: str = "fail") -> Tuple[bool, dict]:
        """Check that grain-defining key columns contain no NULL values.
        
        Args:
            df: DataFrame to check
            key_columns: Columns that define the table grain (e.g. order_id, product_id)
            table_name: Table name used when logging the result
            severity: "fail" blocks the pipeline via raise_if_failed(); "warn" only logs
            
        Returns:
            Tuple of (pass/fail boolean, results dict)
        """
        null_counts = {}
        total_nulls = 0
        for c in key_columns:
            n = df.filter(col(c).isNull()).count()
            null_counts[c] = n
            total_nulls += n
        
        passed = total_nulls == 0
        
        results = {
            "key_columns": key_columns,
            "null_counts": null_counts,
            "total_nulls": total_nulls,
            "passed": passed
        }
        
        if passed:
            print(f"✅ PASSED: No NULLs in key columns {key_columns}")
        elif severity == "warn":
            print(f"⚠️ WARNING: {total_nulls} NULL key values in {key_columns} (non-blocking)")
        else:
            print(f"❌ FAILED: {total_nulls} NULL key values in {key_columns}")
        
        if table_name:
            self.log_results(table_name, "check_null_keys", passed, results, severity)
        return passed, results
    
    def check_valid_dates(self, df: DataFrame, date_columns: List[str],
                          min_date: str, max_date: str,
                          table_name: str = None, severity: str = "fail") -> Tuple[bool, dict]:
        """Check date columns are not NULL and fall within the expected data period.
        
        Args:
            df: DataFrame to check
            date_columns: Date columns to validate
            min_date: Earliest plausible date, e.g. "2016-01-01"
            max_date: Latest plausible date, e.g. "2018-12-31"
            table_name: Table name used when logging the result
            severity: "fail" blocks the pipeline via raise_if_failed(); "warn" only logs
            
        Returns:
            Tuple of (pass/fail boolean, results dict)
        """
        invalid_counts = {}
        total_invalid = 0
        for c in date_columns:
            n = df.filter(
                col(c).isNull()
                | (col(c) < to_date(lit(min_date)))
                | (col(c) > to_date(lit(max_date)))
            ).count()
            invalid_counts[c] = n
            total_invalid += n
        
        passed = total_invalid == 0
        
        results = {
            "date_columns": date_columns,
            "min_date": min_date,
            "max_date": max_date,
            "invalid_counts": invalid_counts,
            "total_invalid": total_invalid,
            "passed": passed
        }
        
        if passed:
            print(f"✅ PASSED: All dates valid within [{min_date}, {max_date}]")
        elif severity == "warn":
            print(f"⚠️ WARNING: {total_invalid} invalid dates (non-blocking)")
        else:
            print(f"❌ FAILED: {total_invalid} invalid dates in {date_columns}")
        
        if table_name:
            self.log_results(table_name, "check_valid_dates", passed, results, severity)
        return passed, results
    
    def check_financial_reconciliation(self, item_df: DataFrame, payment_df: DataFrame,
                                       order_id: str = "order_id",
                                       price_col: str = "price", freight_col: str = "freight_value",
                                       payment_col: str = "payment_value", max_mismatch_pct: float = 1.0,
                                       table_name: str = None, severity: str = "warn") -> Tuple[bool, dict]:
        """Reconcile per-order payment totals against item price + freight.
        
        In the Olist dataset, payment_value = sum(price) + sum(freight_value) per
        order for ~99.6% of orders. Item revenue (price x quantity, EXCLUDES freight)
        and payment totals (INCLUDE freight) are DIFFERENT metrics and must never be
        treated as interchangeable. This check confirms the two sources still
        reconcile within the expected tolerance.
        
        Args:
            item_df: DataFrame with one row per order item
            payment_df: DataFrame with one row per payment installment
            order_id: Join key column name
            price_col: Item price column
            freight_col: Freight column
            payment_col: Payment amount column
            max_mismatch_pct: Maximum tolerated % of orders that differ by >= 0.01
            table_name: Table name used when logging the result
            severity: usually "warn" — small mismatches exist in the source data
            
        Returns:
            Tuple of (pass/fail boolean, results dict)
        """
        item_agg = item_df.groupBy(order_id).agg(
            sum(price_col).alias("_item_total"),
            sum(freight_col).alias("_freight_total")
        )
        pay_agg = payment_df.groupBy(order_id).agg(
            sum(payment_col).alias("_payment_total")
        )
        recon = item_agg.join(pay_agg, order_id)
        
        total_orders = recon.count()
        mismatched = recon.filter(
            abs(col("_payment_total") - (col("_item_total") + col("_freight_total"))) >= 0.01
        ).count()
        mismatch_pct = (100.0 * mismatched / total_orders) if total_orders > 0 else 100.0
        
        passed = mismatch_pct <= max_mismatch_pct
        
        results = {
            "orders_compared": total_orders,
            "orders_mismatched": mismatched,
            "mismatch_percentage": round(mismatch_pct, 3),
            "max_mismatch_pct": max_mismatch_pct,
            "passed": passed
        }
        
        if passed:
            print(f"✅ PASSED: {mismatched}/{total_orders} orders ({round(mismatch_pct, 3)}%) mismatch, tolerance {max_mismatch_pct}%")
        elif severity == "warn":
            print(f"⚠️ WARNING: {mismatched}/{total_orders} orders ({round(mismatch_pct, 3)}%) mismatch (non-blocking)")
        else:
            print(f"❌ FAILED: {mismatched}/{total_orders} orders ({round(mismatch_pct, 3)}%) mismatch, tolerance {max_mismatch_pct}%")
        
        if table_name:
            self.log_results(table_name, "check_financial_reconciliation", passed, results, severity)
        return passed, results
    
    def log_results(self, table_name: str, check_name: str, passed: bool, details: dict,
                    severity: str = "fail"):
        """Log data quality check results.
        
        Args:
            table_name: Name of table checked
            check_name: Name of the quality check
            passed: Whether check passed
            details: Additional check details
            severity: "fail" (blocking) or "warn" (advisory, never stops the pipeline)
        """
        result = {
            "timestamp": datetime.now(),
            "table_name": table_name,
            "check_name": check_name,
            "passed": passed,
            "severity": severity,
            "details": str(details)
        }
        self.results.append(result)
    
    def raise_if_failed(self) -> None:
        """Stop the pipeline if any blocking (severity="fail") check failed.
        
        Warnings (severity="warn") never raise — they are recorded in self.results
        and written to the log table by save_results().
        """
        failures = [r for r in self.results if not r["passed"] and r.get("severity", "fail") == "fail"]
        warnings = [r for r in self.results if not r["passed"] and r.get("severity", "warn") == "warn"]
        
        if failures:
            lines = [f"  - {r['table_name']} :: {r['check_name']} :: {r['details']}"
                     for r in failures]
            raise RuntimeError(
                "DATA QUALITY GATE FAILED — blocking issue(s) detected:\n" + "\n".join(lines)
            )
        if warnings:
            print(f"⚠️ {len(warnings)} non-blocking warning(s) recorded — see data_quality_logs")
    
    def save_results(self, output_table: str = "data_quality_logs"):
        """Save all quality check results to Delta table.
        
        Args:
            output_table: Target table for quality logs
        """
        if self.results:
            results_df = self.spark.createDataFrame(self.results)
            results_df.write.format("delta").mode("append").saveAsTable(output_table)
            print(f"✅ Saved {len(self.results)} quality check results to {output_table}")