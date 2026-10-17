-- Pipeline Monitoring Queries
-- Use these queries to monitor pipeline health and performance

-- ============================================
-- 1. Pipeline Execution Summary
--    (from pipeline_run_log, written by 08_Pipeline_Monitoring;
--     the old pipeline_metrics_log table never existed)
-- ============================================
SELECT 
    run_ts,
    layer,
    primary_table,
    status,
    row_count,
    dq_checks,
    dq_passed,
    dq_failed,
    dq_blocking
FROM pipeline_run_log
ORDER BY run_ts DESC, layer
LIMIT 30;


-- ============================================
-- 2. Data Quality Check Results (Last 7 Days)
-- ============================================
SELECT 
    table_name,
    check_name,
    DATE(timestamp) as check_date,
    COUNT(*) as total_checks,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed_checks,
    SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) as failed_checks,
    ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM data_quality_logs
WHERE timestamp >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY table_name, check_name, DATE(timestamp)
ORDER BY check_date DESC, table_name, check_name;


-- ============================================
-- 3. Table Row Count Trends
-- ============================================
SELECT 
    'bronze_orders' as table_name,
    COUNT(*) as row_count,
    CURRENT_TIMESTAMP() as measured_at
FROM bronze_orders

UNION ALL

SELECT 
    'silver_order_complete' as table_name,
    COUNT(*) as row_count,
    CURRENT_TIMESTAMP() as measured_at
FROM silver_order_complete

UNION ALL

SELECT 
    'gold_product_sales' as table_name,
    COUNT(*) as row_count,
    CURRENT_TIMESTAMP() as measured_at
FROM gold_product_sales;


-- ============================================
-- 4. Data Freshness Check
--    NOTE: only BRONZE tables carry ingestion_time (audit columns are
--    dropped in silver). For silver, the closest available signal is the
--    newest business date (max order_date): data recency, not load recency.
-- ============================================
SELECT 
    'bronze_orders' as table_name,
    MAX(ingestion_time) as latest_ingestion,
    DATEDIFF(HOUR, MAX(ingestion_time), CURRENT_TIMESTAMP()) as hours_since_last_update,
    CASE 
        WHEN DATEDIFF(HOUR, MAX(ingestion_time), CURRENT_TIMESTAMP()) > 24 THEN 'STALE'
        ELSE 'FRESH'
    END as freshness_status
FROM bronze_orders

UNION ALL

SELECT 
    'silver_order_complete' as table_name,
    MAX(order_date) as latest_ingestion,
    DATEDIFF(DAY, MAX(order_date), CURRENT_DATE()) as hours_since_last_update,
    CASE 
        WHEN DATEDIFF(DAY, MAX(order_date), CURRENT_DATE()) > 365 THEN 'STALE'
        ELSE 'FRESH'
    END as freshness_status
FROM silver_order_complete;


-- ============================================
-- 5. Snapshots With Warnings or Failures
-- ============================================
SELECT 
    run_ts,
    layer,
    primary_table,
    status,
    dq_failed,
    dq_blocking
FROM pipeline_run_log
WHERE status IN ('WARN', 'FAIL')
ORDER BY run_ts DESC;


-- ============================================
-- 6. Table Storage Metrics
-- ============================================
DESCRIBE DETAIL bronze_orders;
DESCRIBE DETAIL silver_order_complete;
DESCRIBE DETAIL gold_product_sales;


-- ============================================
-- 7. Data Quality Score by Table
-- ============================================
WITH quality_summary AS (
    SELECT 
        table_name,
        COUNT(*) as total_checks,
        SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed_checks
    FROM data_quality_logs
    WHERE timestamp >= CURRENT_DATE - INTERVAL '7' DAY
    GROUP BY table_name
)
SELECT 
    table_name,
    total_checks,
    passed_checks,
    ROUND(100.0 * passed_checks / total_checks, 2) as quality_score,
    CASE 
        WHEN 100.0 * passed_checks / total_checks >= 95 THEN 'EXCELLENT'
        WHEN 100.0 * passed_checks / total_checks >= 85 THEN 'GOOD'
        WHEN 100.0 * passed_checks / total_checks >= 70 THEN 'FAIR'
        ELSE 'POOR'
    END as quality_rating
FROM quality_summary
ORDER BY quality_score DESC;


-- ============================================
-- 8. Row-Count Trend per Layer (across monitoring snapshots)
-- ============================================
SELECT 
    layer,
    MIN(row_count) as min_rows,
    MAX(row_count) as max_rows,
    COUNT(*) as snapshots_recorded
FROM pipeline_run_log
GROUP BY layer
ORDER BY layer;