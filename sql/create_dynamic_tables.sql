-- ============================================
-- IntelliStock: Dynamic Tables
-- ============================================
-- Purpose: Auto-refreshing analytical tables that update automatically
--          when source data changes. Meets problem statement requirement
--          for "Dynamic Tables (auto-refresh calculations)"
--
-- Benefits:
--   - Eliminates need for manual refresh
--   - Pre-computed analytics for faster dashboard queries
--   - Automatic dependency tracking
--   - Incremental refresh (only changed data)
--
-- Cost Considerations:
--   - Each refresh consumes warehouse credits
--   - Use X-Small or Small warehouse for cost efficiency
--   - Monitor refresh frequency vs. data freshness needs
-- ============================================
-- Switch to the IntelliStock database
USE DATABASE INTELLISTOCK_DB;
USE SCHEMA PUBLIC;
-- ============================================
-- DYNAMIC TABLE 1: STOCK_ANALYTICS_DT
-- ============================================
-- Purpose: Real-time inventory risk analysis
-- Refresh: Every 5 minutes (when base data changes)
-- Dependencies: INVENTORY table
-- 
-- This table pre-computes:
--   - Average daily usage per item/location
--   - Days of stock remaining
--   - Risk status (HIGH/NORMAL)
-- ============================================
CREATE OR REPLACE DYNAMIC TABLE STOCK_ANALYTICS_DT TARGET_LAG = '5 minutes' WAREHOUSE = COMPUTE_WH COMMENT = 'Real-time stock risk analysis with auto-refresh. Updated every 5 minutes.' AS WITH analytics AS (
        SELECT organization,
            location,
            item,
            closing_stock,
            lead_time_days,
            -- Calculate average daily usage using window function
            AVG(issued) OVER (
                PARTITION BY organization,
                location,
                item
                ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as avg_daily_usage,
            -- Get the most recent date for each item
            ROW_NUMBER() OVER (
                PARTITION BY organization,
                location,
                item
                ORDER BY date DESC
            ) as recency_rank
        FROM INVENTORY
    )
SELECT organization,
    location,
    item,
    closing_stock,
    lead_time_days,
    ROUND(avg_daily_usage, 2) as avg_daily_usage,
    -- Calculate days left until stockout
    CASE
        WHEN avg_daily_usage = 0 THEN 9999
        WHEN avg_daily_usage IS NULL THEN 9999
        ELSE ROUND(closing_stock / avg_daily_usage, 2)
    END as days_left,
    -- Determine risk status
    CASE
        WHEN avg_daily_usage = 0 THEN 'NORMAL'
        WHEN avg_daily_usage IS NULL THEN 'NORMAL'
        WHEN closing_stock / avg_daily_usage <= lead_time_days THEN 'HIGH'
        ELSE 'NORMAL'
    END as risk_status,
    CURRENT_TIMESTAMP() as last_updated
FROM analytics
WHERE recency_rank = 1 -- Only keep most recent record per item
;
-- ============================================
-- DYNAMIC TABLE 2: REORDER_RECOMMENDATIONS_DT
-- ============================================
-- Purpose: Auto-calculated reorder quantities and urgency levels
-- Refresh: Every 10 minutes
-- Dependencies: STOCK_ANALYTICS_DT (cascading refresh)
-- 
-- This table provides:
--   - Recommended reorder quantities
--   - Urgency classification (CRITICAL/HIGH/MEDIUM)
--   - Only items that need reordering
-- ============================================
CREATE OR REPLACE DYNAMIC TABLE REORDER_RECOMMENDATIONS_DT TARGET_LAG = '10 minutes' WAREHOUSE = COMPUTE_WH COMMENT = 'Auto-calculated reorder recommendations. Updated every 10 minutes.' AS WITH reorder_calc AS (
        SELECT organization,
            location,
            item,
            closing_stock,
            lead_time_days,
            avg_daily_usage,
            days_left,
            -- Calculate recommended reorder quantity
            -- Formula: (lead_time * daily_usage) - current_stock
            GREATEST(
                0,
                ROUND(
                    (lead_time_days * avg_daily_usage) - closing_stock
                )
            ) as reorder_qty,
            -- Add safety stock (30 days worth)
            GREATEST(
                0,
                ROUND(
                    (lead_time_days + 30) * avg_daily_usage - closing_stock
                )
            ) as reorder_qty_with_safety
        FROM STOCK_ANALYTICS_DT
    ),
    urgency AS (
        SELECT *,
            -- Assign urgency level based on days left
            CASE
                WHEN days_left <= 0 THEN 'CRITICAL'
                WHEN days_left <= lead_time_days * 0.5 THEN 'CRITICAL'
                WHEN days_left <= lead_time_days THEN 'HIGH'
                WHEN days_left <= lead_time_days * 1.5 THEN 'MEDIUM'
                ELSE 'LOW'
            END as urgency_level,
            -- Calculate priority score for sorting
            -- Higher score = more urgent
            ROUND(
                (lead_time_days * 2) + (avg_daily_usage * 1.5) - (closing_stock * 0.5),
                2
            ) as priority_score
        FROM reorder_calc
    )
SELECT organization,
    location,
    item,
    closing_stock,
    avg_daily_usage,
    days_left,
    lead_time_days,
    reorder_qty,
    reorder_qty_with_safety,
    urgency_level,
    priority_score,
    CURRENT_TIMESTAMP() as last_updated
FROM urgency
WHERE reorder_qty > 0 -- Only items that need reordering
ORDER BY priority_score DESC,
    days_left ASC;
-- ============================================
-- DYNAMIC TABLE 3: DAILY_USAGE_STATS_DT
-- ============================================
-- Purpose: Usage pattern analysis and trends
-- Refresh: Every 15 minutes
-- Dependencies: INVENTORY table
-- 
-- This table provides:
--   - 7-day rolling averages
--   - Trend direction (increasing/decreasing/stable)
--   - Peak and minimum usage days
--   - Variance in consumption patterns
-- ============================================
CREATE OR REPLACE DYNAMIC TABLE DAILY_USAGE_STATS_DT TARGET_LAG = '15 minutes' WAREHOUSE = COMPUTE_WH COMMENT = 'Daily usage statistics and trends. Updated every 15 minutes.' AS WITH daily_stats AS (
        SELECT organization,
            location,
            item,
            date,
            issued as daily_issued,
            -- 7-day rolling average
            AVG(issued) OVER (
                PARTITION BY organization,
                location,
                item
                ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as avg_7day,
            -- 30-day rolling average
            AVG(issued) OVER (
                PARTITION BY organization,
                location,
                item
                ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as avg_30day,
            -- Min and Max in last 7 days
            MIN(issued) OVER (
                PARTITION BY organization,
                location,
                item
                ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as min_7day,
            MAX(issued) OVER (
                PARTITION BY organization,
                location,
                item
                ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as max_7day,
            -- Standard deviation
            STDDEV(issued) OVER (
                PARTITION BY organization,
                location,
                item
                ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as stddev_7day,
            ROW_NUMBER() OVER (
                PARTITION BY organization,
                location,
                item
                ORDER BY date DESC
            ) as recency_rank
        FROM INVENTORY
    ),
    trend_analysis AS (
        SELECT *,
            -- Determine trend direction
            CASE
                WHEN avg_7day > avg_30day * 1.1 THEN 'INCREASING'
                WHEN avg_7day < avg_30day * 0.9 THEN 'DECREASING'
                ELSE 'STABLE'
            END as trend_direction,
            -- Calculate coefficient of variation (volatility measure)
            CASE
                WHEN avg_7day = 0 THEN 0
                ELSE ROUND((stddev_7day / avg_7day) * 100, 2)
            END as coefficient_of_variation
        FROM daily_stats
        WHERE recency_rank = 1 -- Most recent data only
    )
SELECT organization,
    location,
    item,
    date as last_data_date,
    ROUND(avg_7day, 2) as avg_daily_usage_7d,
    ROUND(avg_30day, 2) as avg_daily_usage_30d,
    min_7day as min_usage_7d,
    max_7day as max_usage_7d,
    ROUND(stddev_7day, 2) as usage_stddev_7d,
    trend_direction,
    coefficient_of_variation as usage_volatility_pct,
    -- Classify demand pattern
    CASE
        WHEN coefficient_of_variation > 50 THEN 'HIGHLY_VARIABLE'
        WHEN coefficient_of_variation > 25 THEN 'VARIABLE'
        ELSE 'STABLE'
    END as demand_pattern,
    CURRENT_TIMESTAMP() as last_updated
FROM trend_analysis;
-- ============================================
-- VERIFICATION QUERIES
-- ============================================
-- Check dynamic table creation status
SHOW DYNAMIC TABLES IN SCHEMA PUBLIC;
-- View refresh history for STOCK_ANALYTICS_DT
-- (Uncomment after tables are created and have refreshed at least once)
/*
 SELECT 
 name,
 state,
 target_lag,
 data_timestamp,
 refresh_trigger,
 last_refresh_start_time,
 last_refresh_end_time
 FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
 NAME => 'STOCK_ANALYTICS_DT'
 ));
 */
-- Sample query: Get high-risk items from dynamic table
-- (Should be instant, no CTE computation needed)
/*
 SELECT 
 organization,
 location,
 item,
 closing_stock,
 days_left,
 risk_status
 FROM STOCK_ANALYTICS_DT
 WHERE risk_status = 'HIGH'
 ORDER BY days_left ASC
 LIMIT 10;
 */
-- Sample query: Get top reorder recommendations
/*
 SELECT 
 organization,
 location,
 item,
 reorder_qty,
 urgency_level,
 priority_score
 FROM REORDER_RECOMMENDATIONS_DT
 WHERE urgency_level IN ('CRITICAL', 'HIGH')
 ORDER BY priority_score DESC
 LIMIT 10;
 */
-- Sample query: Check usage trends
/*
 SELECT 
 organization,
 item,
 avg_daily_usage_7d,
 avg_daily_usage_30d,
 trend_direction,
 demand_pattern
 FROM DAILY_USAGE_STATS_DT
 WHERE trend_direction = 'INCREASING'
 ORDER BY avg_daily_usage_7d DESC
 LIMIT 10;
 */
-- ============================================
-- MAINTENANCE COMMANDS
-- ============================================
-- Manually refresh a dynamic table (if needed)
-- ALTER DYNAMIC TABLE STOCK_ANALYTICS_DT REFRESH;
-- Suspend auto-refresh (to save costs)
-- ALTER DYNAMIC TABLE STOCK_ANALYTICS_DT SUSPEND;
-- Resume auto-refresh
-- ALTER DYNAMIC TABLE STOCK_ANALYTICS_DT RESUME;
-- Change refresh frequency
-- ALTER DYNAMIC TABLE STOCK_ANALYTICS_DT SET TARGET_LAG = '10 minutes';
-- Drop dynamic tables (if needed for cleanup)
-- DROP DYNAMIC TABLE IF EXISTS DAILY_USAGE_STATS_DT;
-- DROP DYNAMIC TABLE IF EXISTS REORDER_RECOMMENDATIONS_DT;
-- DROP DYNAMIC TABLE IF EXISTS STOCK_ANALYTICS_DT;
-- ============================================
-- NOTES FOR DEPLOYMENT
-- ============================================
-- 1. Update WAREHOUSE name if different from COMPUTE_WH
-- 2. Adjust TARGET_LAG based on data freshness needs
-- 3. Monitor credit consumption for the first 24 hours
-- 4. For production: Use separate warehouse for dynamic tables
-- 5. Consider auto-suspend settings on warehouse for cost control