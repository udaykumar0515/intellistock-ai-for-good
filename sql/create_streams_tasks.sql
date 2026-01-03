-- ============================================
-- IntelliStock: Streams & Tasks
-- ============================================
-- Purpose: Implement change data capture (Streams) and scheduled
--          automation (Tasks) for event-driven processing.
--          Meets problem statement requirement for
--          "Streams & Tasks (scheduling)"
--
-- Architecture:
--   INVENTORY table → INVENTORY_CHANGES stream 
--                  → TASK_REFRESH_ANALYTICS_SUMMARY (15 min)
--                  → TASK_GENERATE_ALERTS (after parent)
--   
--   Independent: TASK_DAILY_CLEANUP (daily 2am)
--
-- Benefits:
--   - Event-driven processing (only when data changes)
--   - Automated scheduled refreshes
--   - Eliminates manual intervention
--   - Tracks change history
-- ============================================
-- Switch to the IntelliStock database
USE DATABASE INTELLISTOCK_DB;
USE SCHEMA PUBLIC;
-- ============================================
-- PART 1: STREAMS (Change Data Capture)
-- ============================================
-- ============================================
-- STREAM 1: INVENTORY_CHANGES
-- ============================================
-- Purpose: Track all changes to the INVENTORY table
-- Type: Standard stream (tracks INSERTs, UPDATEs, DELETEs)
-- Used by: Tasks to trigger downstream processing
-- ============================================
CREATE OR REPLACE STREAM INVENTORY_CHANGES ON TABLE INVENTORY COMMENT = 'Tracks all changes to the INVENTORY table for event-driven processing';
-- ============================================
-- STREAM 2: ANALYTICS_CHANGES
-- ============================================
-- Purpose: Track changes to the stock analytics dynamic table
-- Type: Standard stream on dynamic table
-- Used by: Alert generation task
-- ============================================
CREATE OR REPLACE STREAM ANALYTICS_CHANGES ON DYNAMIC TABLE STOCK_ANALYTICS_DT COMMENT = 'Tracks changes to stock analytics for alert generation';
-- ============================================
-- PART 2: SUPPORTING TABLES
-- ============================================
-- These tables store results from task executions
-- Table: ANALYTICS_SUMMARY
-- Purpose: Store daily/hourly analytics summaries
CREATE TABLE IF NOT EXISTS ANALYTICS_SUMMARY (
    summary_id INTEGER AUTOINCREMENT,
    summary_date DATE,
    summary_hour INTEGER,
    total_organizations INTEGER,
    total_items INTEGER,
    high_risk_count INTEGER,
    critical_risk_count INTEGER,
    total_reorder_qty FLOAT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (summary_id)
);
-- Table: ALERT_HISTORY
-- Purpose: Store history of all generated alerts
CREATE TABLE IF NOT EXISTS ALERT_HISTORY (
    alert_id INTEGER AUTOINCREMENT,
    organization STRING,
    location STRING,
    item STRING,
    alert_type STRING,
    -- 'HIGH_RISK', 'CRITICAL_RISK', 'REORDER_NEEDED'
    days_left FLOAT,
    closing_stock INTEGER,
    reorder_qty FLOAT,
    priority_score FLOAT,
    alert_date DATE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (alert_id)
);
-- Table: TASK_EXECUTION_LOG
-- Purpose: Log all task executions for monitoring
CREATE TABLE IF NOT EXISTS TASK_EXECUTION_LOG (
    log_id INTEGER AUTOINCREMENT,
    task_name STRING,
    execution_time TIMESTAMP_NTZ,
    status STRING,
    -- 'SUCCESS', 'FAILED', 'SKIPPED'
    records_processed INTEGER,
    error_message STRING,
    execution_duration_seconds FLOAT,
    PRIMARY KEY (log_id)
);
-- ============================================
-- PART 3: STORED PROCEDURES
-- ============================================
-- These procedures are called by tasks
-- ============================================
-- PROCEDURE: SP_REFRESH_ANALYTICS_SUMMARY
-- ============================================
-- Purpose: Refresh analytics summary table with latest metrics
-- Called by: TASK_REFRESH_ANALYTICS_SUMMARY
-- ============================================
CREATE OR REPLACE PROCEDURE SP_REFRESH_ANALYTICS_SUMMARY() RETURNS STRING LANGUAGE SQL AS $$
DECLARE start_time TIMESTAMP_NTZ;
end_time TIMESTAMP_NTZ;
records_inserted INTEGER;
error_msg STRING;
BEGIN -- Record start time
start_time := CURRENT_TIMESTAMP();
-- Insert summary for current hour
INSERT INTO ANALYTICS_SUMMARY (
        summary_date,
        summary_hour,
        total_organizations,
        total_items,
        high_risk_count,
        critical_risk_count,
        total_reorder_qty
    )
SELECT CURRENT_DATE() as summary_date,
    HOUR(CURRENT_TIMESTAMP()) as summary_hour,
    COUNT(DISTINCT organization) as total_organizations,
    COUNT(DISTINCT item) as total_items,
    SUM(
        CASE
            WHEN risk_status = 'HIGH' THEN 1
            ELSE 0
        END
    ) as high_risk_count,
    SUM(
        CASE
            WHEN days_left <= 0 THEN 1
            ELSE 0
        END
    ) as critical_risk_count,
    COALESCE(SUM(reorder_qty), 0) as total_reorder_qty
FROM STOCK_ANALYTICS_DT sa
    LEFT JOIN REORDER_RECOMMENDATIONS_DT rr ON sa.organization = rr.organization
    AND sa.location = rr.location
    AND sa.item = rr.item;
records_inserted := SQLROWCOUNT;
end_time := CURRENT_TIMESTAMP();
-- Log execution
INSERT INTO TASK_EXECUTION_LOG (
        task_name,
        execution_time,
        status,
        records_processed,
        execution_duration_seconds
    )
VALUES (
        'SP_REFRESH_ANALYTICS_SUMMARY',
        start_time,
        'SUCCESS',
        records_inserted,
        DATEDIFF('second', start_time, end_time)
    );
RETURN 'Summary refreshed successfully. Records inserted: ' || records_inserted;
EXCEPTION
WHEN OTHER THEN error_msg := SQLERRM;
INSERT INTO TASK_EXECUTION_LOG (
        task_name,
        execution_time,
        status,
        records_processed,
        error_message
    )
VALUES (
        'SP_REFRESH_ANALYTICS_SUMMARY',
        start_time,
        'FAILED',
        0,
        error_msg
    );
RETURN 'Error: ' || error_msg;
END;
$$;
-- ============================================
-- PROCEDURE: SP_GENERATE_ALERTS
-- ============================================
-- Purpose: Generate alerts for high-risk and critical items
-- Called by: TASK_GENERATE_ALERTS
-- ============================================
CREATE OR REPLACE PROCEDURE SP_GENERATE_ALERTS() RETURNS STRING LANGUAGE SQL AS $$
DECLARE start_time TIMESTAMP_NTZ;
end_time TIMESTAMP_NTZ;
records_inserted INTEGER;
error_msg STRING;
BEGIN start_time := CURRENT_TIMESTAMP();
-- Insert new alerts for today (avoid duplicates)
INSERT INTO ALERT_HISTORY (
        organization,
        location,
        item,
        alert_type,
        days_left,
        closing_stock,
        reorder_qty,
        priority_score,
        alert_date
    )
SELECT sa.organization,
    sa.location,
    sa.item,
    CASE
        WHEN sa.days_left <= 0 THEN 'CRITICAL_RISK'
        WHEN sa.risk_status = 'HIGH' THEN 'HIGH_RISK'
        WHEN rr.urgency_level = 'CRITICAL' THEN 'CRITICAL_RISK'
        ELSE 'REORDER_NEEDED'
    END as alert_type,
    sa.days_left,
    sa.closing_stock,
    COALESCE(rr.reorder_qty, 0) as reorder_qty,
    COALESCE(rr.priority_score, 0) as priority_score,
    CURRENT_DATE() as alert_date
FROM STOCK_ANALYTICS_DT sa
    LEFT JOIN REORDER_RECOMMENDATIONS_DT rr ON sa.organization = rr.organization
    AND sa.location = rr.location
    AND sa.item = rr.item
WHERE sa.risk_status = 'HIGH'
    OR sa.days_left <= 3 -- Avoid duplicates: only insert if not already alerted today
    AND NOT EXISTS (
        SELECT 1
        FROM ALERT_HISTORY ah
        WHERE ah.organization = sa.organization
            AND ah.location = sa.location
            AND ah.item = sa.item
            AND ah.alert_date = CURRENT_DATE()
    );
records_inserted := SQLROWCOUNT;
end_time := CURRENT_TIMESTAMP();
-- Log execution
INSERT INTO TASK_EXECUTION_LOG (
        task_name,
        execution_time,
        status,
        records_processed,
        execution_duration_seconds
    )
VALUES (
        'SP_GENERATE_ALERTS',
        start_time,
        'SUCCESS',
        records_inserted,
        DATEDIFF('second', start_time, end_time)
    );
RETURN 'Alerts generated successfully. New alerts: ' || records_inserted;
EXCEPTION
WHEN OTHER THEN error_msg := SQLERRM;
INSERT INTO TASK_EXECUTION_LOG (
        task_name,
        execution_time,
        status,
        records_processed,
        error_message
    )
VALUES (
        'SP_GENERATE_ALERTS',
        start_time,
        'FAILED',
        0,
        error_msg
    );
RETURN 'Error: ' || error_msg;
END;
$$;
-- ============================================
-- PART 4: TASKS (Scheduled Automation)
-- ============================================
-- ============================================
-- TASK 1: TASK_REFRESH_ANALYTICS_SUMMARY (Root Task)
-- ============================================
-- Schedule: Every 15 minutes
-- Trigger: When INVENTORY_CHANGES stream has data
-- Dependencies: None (root task)
-- Purpose: Refresh analytics summary when inventory changes
-- ============================================
CREATE OR REPLACE TASK TASK_REFRESH_ANALYTICS_SUMMARY WAREHOUSE = COMPUTE_WH SCHEDULE = '15 minute' COMMENT = 'Refreshes analytics summary every 15 minutes when inventory changes detected'
    WHEN -- Only run if there are changes in the inventory
    SYSTEM $STREAM_HAS_DATA('INVENTORY_CHANGES') AS CALL SP_REFRESH_ANALYTICS_SUMMARY();
-- ============================================
-- TASK 2: TASK_GENERATE_ALERTS (Child Task)
-- ============================================
-- Schedule: Runs after TASK_REFRESH_ANALYTICS_SUMMARY completes
-- Trigger: Parent task completion + stream has data
-- Dependencies: TASK_REFRESH_ANALYTICS_SUMMARY
-- Purpose: Generate alerts for high-risk items
-- ============================================
CREATE OR REPLACE TASK TASK_GENERATE_ALERTS WAREHOUSE = COMPUTE_WH COMMENT = 'Generates alerts for high-risk items after analytics refresh'
AFTER TASK_REFRESH_ANALYTICS_SUMMARY
    WHEN -- Only run if analytics have changed
    SYSTEM $STREAM_HAS_DATA('ANALYTICS_CHANGES') AS CALL SP_GENERATE_ALERTS();
-- ============================================
-- TASK 3: TASK_DAILY_CLEANUP (Independent Task)
-- ============================================
-- Schedule: Daily at 2:00 AM
-- Trigger: Time-based (not event-driven)
-- Dependencies: None (independent)
-- Purpose: Archive old data, cleanup temp tables
-- ============================================
CREATE OR REPLACE TASK TASK_DAILY_CLEANUP WAREHOUSE = COMPUTE_WH SCHEDULE = 'USING CRON 0 2 * * * UTC' COMMENT = 'Daily cleanup and archival task, runs at 2:00 AM UTC' AS BEGIN -- Archive old analytics summaries (keep last 90 days)
DELETE FROM ANALYTICS_SUMMARY
WHERE summary_date < DATEADD('day', -90, CURRENT_DATE());
-- Archive old alerts (keep last 180 days)
DELETE FROM ALERT_HISTORY
WHERE alert_date < DATEADD('day', -180, CURRENT_DATE());
-- Archive old task logs (keep last 30 days)
DELETE FROM TASK_EXECUTION_LOG
WHERE execution_time < DATEADD('day', -30, CURRENT_TIMESTAMP());
-- Log cleanup execution
INSERT INTO TASK_EXECUTION_LOG (
        task_name,
        execution_time,
        status,
        records_processed
    )
VALUES (
        'TASK_DAILY_CLEANUP',
        CURRENT_TIMESTAMP(),
        'SUCCESS',
        SQLROWCOUNT
    );
END;
-- ============================================
-- PART 5: TASK ACTIVATION
-- ============================================
-- Tasks are created in SUSPENDED state by default
-- Must be RESUMED to start execution
-- Resume root task first
ALTER TASK TASK_REFRESH_ANALYTICS_SUMMARY RESUME;
-- Resume child task (automatically starts when parent completes)
ALTER TASK TASK_GENERATE_ALERTS RESUME;
-- Resume independent cleanup task
ALTER TASK TASK_DAILY_CLEANUP RESUME;
-- ============================================
-- VERIFICATION QUERIES
-- ============================================
-- Check stream status
SHOW STREAMS IN SCHEMA PUBLIC;
-- Check if streams have data
SELECT SYSTEM $STREAM_HAS_DATA('INVENTORY_CHANGES') as inventory_has_changes;
SELECT SYSTEM $STREAM_HAS_DATA('ANALYTICS_CHANGES') as analytics_has_changes;
-- View current data in streams (first 10 rows)
-- SELECT * FROM INVENTORY_CHANGES LIMIT 10;
-- Check task status
SHOW TASKS IN SCHEMA PUBLIC;
-- View task execution history (last 24 hours)
/*
 SELECT 
 name,
 state,
 scheduled_time,
 completed_time,
 return_value
 FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
 SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
 RESULT_LIMIT => 20
 ))
 ORDER BY scheduled_time DESC;
 */
-- Check analytics summary table
SELECT *
FROM ANALYTICS_SUMMARY
ORDER BY created_at DESC
LIMIT 10;
-- Check alert history
SELECT *
FROM ALERT_HISTORY
ORDER BY created_at DESC
LIMIT 10;
-- Check task execution logs
SELECT *
FROM TASK_EXECUTION_LOG
ORDER BY execution_time DESC
LIMIT 10;
-- ============================================
-- MANAGEMENT COMMANDS
-- ============================================
-- Suspend all tasks (to save costs or troubleshoot)
-- ALTER TASK TASK_REFRESH_ANALYTICS_SUMMARY SUSPEND;
-- ALTER TASK TASK_GENERATE_ALERTS SUSPEND;
-- ALTER TASK TASK_DAILY_CLEANUP SUSPEND;
-- Resume all tasks
-- ALTER TASK TASK_REFRESH_ANALYTICS_SUMMARY RESUME;
-- ALTER TASK TASK_GENERATE_ALERTS RESUME;
-- ALTER TASK TASK_DAILY_CLEANUP RESUME;
-- Execute task manually (for testing)
-- EXECUTE TASK TASK_REFRESH_ANALYTICS_SUMMARY;
-- View task dependencies
-- SHOW TASKS LIKE 'TASK_%';
-- Drop tasks (if needed - must drop children first)
-- ALTER TASK TASK_GENERATE_ALERTS SUSPEND;
-- DROP TASK IF EXISTS TASK_GENERATE_ALERTS;
-- ALTER TASK TASK_REFRESH_ANALYTICS_SUMMARY SUSPEND;
-- DROP TASK IF EXISTS TASK_REFRESH_ANALYTICS_SUMMARY;
-- ALTER TASK TASK_DAILY_CLEANUP SUSPEND;
-- DROP TASK IF EXISTS TASK_DAILY_CLEANUP;
-- Drop streams (if needed)
-- DROP STREAM IF EXISTS ANALYTICS_CHANGES;
-- DROP STREAM IF EXISTS INVENTORY_CHANGES;
-- ============================================
-- MONITORING QUERIES
-- ============================================
-- Monitor task execution success rate
/*
 SELECT 
 task_name,
 COUNT(*) as total_executions,
 SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
 SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
 ROUND(AVG(execution_duration_seconds), 2) as avg_duration_sec
 FROM TASK_EXECUTION_LOG
 WHERE execution_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
 GROUP BY task_name;
 */
-- Find failed task executions
/*
 SELECT * 
 FROM TASK_EXECUTION_LOG
 WHERE status = 'FAILED'
 ORDER BY execution_time DESC
 LIMIT 10;
 */
-- ============================================
-- NOTES FOR DEPLOYMENT
-- ============================================
-- 1. Update WAREHOUSE name if different from COMPUTE_WH
-- 2. Adjust task schedules based on your needs
-- 3. Monitor credit consumption after enabling tasks
-- 4. For production: Use separate warehouse for tasks
-- 5. Set up email alerts for failed tasks (Snowflake feature)
-- 6. Test with small data first before production deployment
-- 7. Consider warehouse auto-suspend settings