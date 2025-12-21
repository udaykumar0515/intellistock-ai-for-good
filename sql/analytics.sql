-- IntelliStock Analytics Queries
-- All analytics logic is defined in SQL (source of truth)
-- ==========================================
-- QUERY 1: Overview Metrics
-- ==========================================
-- Returns total organizations, total items, and HIGH-risk count
WITH analytics AS (
    SELECT organization,
        location,
        item,
        closing_stock,
        lead_time_days,
        AVG(issued) OVER (PARTITION BY organization, location, item) as avg_daily_usage
    FROM INVENTORY
),
risk_analysis AS (
    SELECT organization,
        location,
        item,
        closing_stock,
        lead_time_days,
        avg_daily_usage,
        CASE
            WHEN avg_daily_usage = 0 THEN 9999
            ELSE closing_stock / avg_daily_usage
        END as days_left
    FROM analytics
),
risk_status AS (
    SELECT *,
        CASE
            WHEN days_left <= lead_time_days THEN 'HIGH'
            ELSE 'NORMAL'
        END as risk_status
    FROM risk_analysis
)
SELECT COUNT(DISTINCT organization) as total_organizations,
    COUNT(DISTINCT item) as total_items,
    SUM(
        CASE
            WHEN risk_status = 'HIGH' THEN 1
            ELSE 0
        END
    ) as high_risk_count
FROM risk_status;
-- ==========================================
-- QUERY 2: Inventory Heatmap Data
-- ==========================================
-- Returns closing stock aggregated by item and location
SELECT item,
    location,
    SUM(closing_stock) as total_closing_stock
FROM INVENTORY
GROUP BY item,
    location
ORDER BY item,
    location;
-- ==========================================
-- QUERY 3: Stock-Out Alerts
-- ==========================================
-- Returns only HIGH-risk items with detailed metrics
WITH analytics AS (
    SELECT organization,
        location,
        item,
        closing_stock,
        lead_time_days,
        AVG(issued) OVER (PARTITION BY organization, location, item) as avg_daily_usage
    FROM INVENTORY
),
risk_analysis AS (
    SELECT organization,
        location,
        item,
        closing_stock,
        lead_time_days,
        avg_daily_usage,
        CASE
            WHEN avg_daily_usage = 0 THEN 9999
            ELSE closing_stock / avg_daily_usage
        END as days_left
    FROM analytics
),
risk_status AS (
    SELECT *,
        CASE
            WHEN days_left <= lead_time_days THEN 'HIGH'
            ELSE 'NORMAL'
        END as risk_status
    FROM risk_analysis
)
SELECT organization,
    location,
    item,
    closing_stock,
    ROUND(avg_daily_usage, 2) as avg_daily_usage,
    ROUND(days_left, 2) as days_left,
    lead_time_days,
    risk_status
FROM risk_status
WHERE risk_status = 'HIGH'
ORDER BY days_left ASC;
-- ==========================================
-- QUERY 4: Reorder Recommendations
-- ==========================================
-- Returns items needing reorder with calculated quantities
WITH analytics AS (
    SELECT organization,
        location,
        item,
        closing_stock,
        lead_time_days,
        AVG(issued) OVER (PARTITION BY organization, location, item) as avg_daily_usage
    FROM INVENTORY
),
risk_analysis AS (
    SELECT organization,
        location,
        item,
        closing_stock,
        lead_time_days,
        avg_daily_usage,
        CASE
            WHEN avg_daily_usage = 0 THEN 9999
            ELSE closing_stock / avg_daily_usage
        END as days_left
    FROM analytics
),
reorder_calc AS (
    SELECT *,
        GREATEST(
            0,
            (lead_time_days * avg_daily_usage) - closing_stock
        ) as reorder_qty
    FROM risk_analysis
),
urgency AS (
    SELECT *,
        CASE
            WHEN days_left <= 0 THEN 'CRITICAL'
            WHEN days_left <= lead_time_days * 0.5 THEN 'CRITICAL'
            WHEN days_left <= lead_time_days THEN 'HIGH'
            ELSE 'MEDIUM'
        END as urgency_level
    FROM reorder_calc
)
SELECT organization,
    location,
    item,
    closing_stock,
    ROUND(avg_daily_usage, 2) as avg_daily_usage,
    ROUND(days_left, 2) as days_left,
    lead_time_days,
    ROUND(reorder_qty) as reorder_qty,
    urgency_level
FROM urgency
WHERE reorder_qty > 0
ORDER BY urgency_level DESC,
    days_left ASC;
-- ==========================================
-- QUERY 5: Get Latest Inventory Snapshot
-- ==========================================
-- Returns the most recent data for each item/location combination
WITH latest_date AS (
    SELECT MAX(date) as max_date
    FROM INVENTORY
)
SELECT i.date,
    i.organization,
    i.location,
    i.item,
    i.opening_stock,
    i.received,
    i.issued,
    i.closing_stock,
    i.lead_time_days
FROM INVENTORY i
    CROSS JOIN latest_date ld
WHERE i.date = ld.max_date
ORDER BY i.organization,
    i.location,
    i.item;