-- ============================================
-- IntelliStock: Unistore Hybrid Tables
-- ============================================
-- Purpose: Implement Unistore hybrid tables for high-performance
--          transactional workloads. Meets optional problem statement
--          requirement for "Unistore for action logs"
--
-- Unistore Benefits:
--   - Row-level locking (ACID transactions)
--   - Low-latency writes (<100ms)
--   - Optimized for single-row lookups
--   - Mixed analytical and transactional workloads
--   - Primary key enforcement
--
-- Use Cases:
--   - Action logging (orders, exports, reviews)
--   - User session tracking
--   - Audit trail for compliance
--   - Real-time operational data
-- ============================================
-- Switch to the IntelliStock database
USE DATABASE INTELLISTOCK_DB;
USE SCHEMA PUBLIC;
-- ============================================
-- HYBRID TABLE 1: ACTION_LOG
-- ============================================
-- Purpose: Log all user actions in real-time
-- Type: Unistore Hybrid Table
-- Performance: Optimized for INSERT and single-row SELECT
-- 
-- Actions logged:
--   - ORDER_PLACED: User marked item as ordered
--   - ITEM_REVIEWED: User reviewed item details
--   - PDF_EXPORTED: User exported action panel PDF
--   - CONFIG_CHANGED: User modified criticality settings
--   - DATA_UPLOADED: User uploaded new inventory data
-- ============================================
CREATE OR REPLACE HYBRID TABLE ACTION_LOG (
        -- Primary key (auto-incrementing)
        action_id INTEGER AUTOINCREMENT,
        -- Action metadata
        action_type STRING NOT NULL,
        -- 'ORDER_PLACED', 'PDF_EXPORTED', etc.
        action_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
        -- User information
        user_name STRING,
        -- From Streamlit session or Snowflake context
        user_email STRING,
        session_id STRING,
        -- Streamlit session ID for tracking
        -- Item information (if applicable)
        organization STRING,
        location STRING,
        item STRING,
        -- Action details (JSON for flexibility)
        action_details VARIANT,
        -- Stores additional metadata as JSON
        -- Performance tracking
        response_time_ms INTEGER,
        -- How long the action took
        -- Source tracking
        source_page STRING,
        -- Which Streamlit page triggered the action
        source_ip STRING,
        -- User IP (if available)
        -- Status
        status STRING DEFAULT 'SUCCESS',
        -- 'SUCCESS', 'FAILED', 'PENDING'
        error_message STRING,
        -- Constraints
        PRIMARY KEY (action_id),
        -- Indexes for common queries
        INDEX idx_action_type (action_type),
        INDEX idx_timestamp (action_timestamp),
        INDEX idx_user (user_name),
        INDEX idx_item (organization, location, item)
    ) COMMENT = 'Unistore hybrid table for real-time action logging with ACID guarantees';
-- ============================================
-- HYBRID TABLE 2: USER_SESSION_LOG
-- ============================================
-- Purpose: Track user sessions and interactions
-- Type: Unistore Hybrid Table
-- Performance: Optimized for session lookups and updates
-- 
-- Tracks:
--   - Session start/end times
--   - Pages visited
--   - Actions performed
--   - Total session duration
-- ============================================
CREATE OR REPLACE HYBRID TABLE USER_SESSION_LOG (
        -- Primary key
        session_id STRING NOT NULL,
        -- User information
        user_name STRING,
        user_email STRING,
        -- Session timing
        session_start TIMESTAMP_NTZ NOT NULL,
        session_end TIMESTAMP_NTZ,
        last_activity TIMESTAMP_NTZ,
        session_duration_minutes INTEGER,
        -- Activity tracking
        pages_visited ARRAY,
        -- List of pages visited
        actions_count INTEGER DEFAULT 0,
        -- Session details
        browser STRING,
        device_type STRING,
        -- 'desktop', 'mobile', 'tablet'
        ip_address STRING,
        -- Business metrics
        items_reviewed INTEGER DEFAULT 0,
        items_ordered INTEGER DEFAULT 0,
        pdfs_exported INTEGER DEFAULT 0,
        data_uploads INTEGER DEFAULT 0,
        -- Session state
        is_active BOOLEAN DEFAULT TRUE,
        -- Constraints
        PRIMARY KEY (session_id),
        -- Indexes
        INDEX idx_user (user_name),
        INDEX idx_start_time (session_start),
        INDEX idx_active (is_active)
    ) COMMENT = 'Unistore hybrid table for user session tracking and audit trails';
-- ============================================
-- HYBRID TABLE 3: ORDER_TRACKING
-- ============================================
-- Purpose: Track items marked as ordered with full audit trail
-- Type: Unistore Hybrid Table
-- Performance: Optimized for order status lookups
-- 
-- Provides:
--   - Real-time order status
--   - Order history and modifications
--   - Integration with procurement systems
-- ============================================
CREATE OR REPLACE HYBRID TABLE ORDER_TRACKING (
        -- Primary key
        order_id INTEGER AUTOINCREMENT,
        -- Item identification
        organization STRING NOT NULL,
        location STRING NOT NULL,
        item STRING NOT NULL,
        -- Order details
        order_date DATE NOT NULL DEFAULT CURRENT_DATE(),
        order_timestamp TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
        ordered_by STRING,
        -- User who placed the order
        -- Quantities
        current_stock INTEGER,
        recommended_qty FLOAT,
        ordered_qty FLOAT,
        -- Status tracking
        status STRING DEFAULT 'PENDING',
        -- 'PENDING', 'CONFIRMED', 'SHIPPED', 'RECEIVED', 'CANCELLED'
        priority_level STRING,
        -- 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'
        -- Delivery tracking
        expected_delivery_date DATE,
        actual_delivery_date DATE,
        supplier STRING,
        -- Financial
        unit_cost FLOAT,
        total_cost FLOAT,
        -- Audit trail
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        modified_at TIMESTAMP_NTZ,
        modified_by STRING,
        -- Notes
        notes STRING,
        -- Constraints
        PRIMARY KEY (order_id),
        -- Indexes for fast lookups
        INDEX idx_item_lookup (organization, location, item),
        INDEX idx_status (status),
        INDEX idx_order_date (order_date),
        INDEX idx_delivery (expected_delivery_date)
    ) COMMENT = 'Unistore hybrid table for order tracking with full audit trail';
-- ============================================
-- VIEWS FOR ANALYTICS
-- ============================================
-- View: Recent Actions (Last 24 hours)
CREATE OR REPLACE VIEW VW_RECENT_ACTIONS AS
SELECT action_id,
    action_type,
    action_timestamp,
    user_name,
    organization,
    location,
    item,
    status,
    source_page
FROM ACTION_LOG
WHERE action_timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY action_timestamp DESC;
-- View: Active User Sessions
CREATE OR REPLACE VIEW VW_ACTIVE_SESSIONS AS
SELECT session_id,
    user_name,
    session_start,
    last_activity,
    DATEDIFF(
        'minute',
        session_start,
        COALESCE(last_activity, CURRENT_TIMESTAMP())
    ) as minutes_active,
    pages_visited,
    actions_count,
    items_ordered
FROM USER_SESSION_LOG
WHERE is_active = TRUE
ORDER BY last_activity DESC;
-- View: Pending Orders
CREATE OR REPLACE VIEW VW_PENDING_ORDERS AS
SELECT order_id,
    organization,
    location,
    item,
    order_date,
    ordered_qty,
    priority_level,
    expected_delivery_date,
    DATEDIFF('day', CURRENT_DATE(), expected_delivery_date) as days_until_delivery
FROM ORDER_TRACKING
WHERE status IN ('PENDING', 'CONFIRMED', 'SHIPPED')
ORDER BY priority_level DESC,
    expected_delivery_date ASC;
-- ============================================
-- HELPER PROCEDURES FOR UNISTORE OPERATIONS
-- ============================================
-- ============================================
-- PROCEDURE: SP_LOG_ACTION
-- ============================================
-- Purpose: Helper procedure to log actions from Streamlit
-- Usage: CALL SP_LOG_ACTION('ORDER_PLACED', 'user@example.com', ...)
-- ============================================
CREATE OR REPLACE PROCEDURE SP_LOG_ACTION(
        p_action_type STRING,
        p_user_name STRING,
        p_organization STRING,
        p_location STRING,
        p_item STRING,
        p_details VARIANT,
        p_session_id STRING
    ) RETURNS STRING LANGUAGE SQL AS $$ BEGIN
INSERT INTO ACTION_LOG (
        action_type,
        user_name,
        organization,
        location,
        item,
        action_details,
        session_id,
        source_page
    )
VALUES (
        p_action_type,
        p_user_name,
        p_organization,
        p_location,
        p_item,
        p_details,
        p_session_id,
        'Dashboard' -- Default, can be parameterized
    );
-- Update session activity count
UPDATE USER_SESSION_LOG
SET actions_count = actions_count + 1,
    last_activity = CURRENT_TIMESTAMP()
WHERE session_id = p_session_id;
RETURN 'Action logged successfully';
END;
$$;
-- ============================================
-- PROCEDURE: SP_CREATE_ORDER
-- ============================================
-- Purpose: Create a new order entry
-- Usage: CALL SP_CREATE_ORDER('City Hospital', 'ER', 'Paracetamol', ...)
-- ============================================
CREATE OR REPLACE PROCEDURE SP_CREATE_ORDER(
        p_organization STRING,
        p_location STRING,
        p_item STRING,
        p_ordered_qty FLOAT,
        p_user_name STRING,
        p_priority STRING
    ) RETURNS STRING LANGUAGE SQL AS $$
DECLARE v_order_id INTEGER;
BEGIN -- Insert order
INSERT INTO ORDER_TRACKING (
        organization,
        location,
        item,
        ordered_by,
        ordered_qty,
        priority_level,
        status
    )
VALUES (
        p_organization,
        p_location,
        p_item,
        p_user_name,
        p_ordered_qty,
        p_priority,
        'PENDING'
    );
v_order_id := (
    SELECT MAX(order_id)
    FROM ORDER_TRACKING
);
-- Log the action
INSERT INTO ACTION_LOG (
        action_type,
        user_name,
        organization,
        location,
        item,
        action_details
    )
VALUES (
        'ORDER_PLACED',
        p_user_name,
        p_organization,
        p_location,
        p_item,
        OBJECT_CONSTRUCT(
            'order_id',
            v_order_id,
            'quantity',
            p_ordered_qty
        )
    );
RETURN 'Order created successfully. Order ID: ' || v_order_id;
END;
$$;
-- ============================================
-- VERIFICATION QUERIES
-- ============================================
-- Check hybrid tables
SHOW HYBRID TABLES IN SCHEMA PUBLIC;
-- Test insert into ACTION_LOG
/*
 INSERT INTO ACTION_LOG (
 action_type,
 user_name,
 organization,
 location,
 item,
 action_details
 )
 VALUES (
 'ORDER_PLACED',
 'test_user',
 'City Hospital',
 'Emergency Unit',
 'Paracetamol',
 OBJECT_CONSTRUCT('quantity', 100, 'priority', 'HIGH')
 );
 */
-- Query recent actions
SELECT *
FROM VW_RECENT_ACTIONS
LIMIT 10;
-- Check action statistics by type
/*
 SELECT 
 action_type,
 COUNT(*) as action_count,
 COUNT(DISTINCT user_name) as unique_users
 FROM ACTION_LOG
 WHERE action_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
 GROUP BY action_type
 ORDER BY action_count DESC;
 */
-- Check active sessions
SELECT *
FROM VW_ACTIVE_SESSIONS;
-- Check pending orders
SELECT *
FROM VW_PENDING_ORDERS;
-- ============================================
-- SAMPLE DATA (For Testing)
-- ============================================
-- Insert sample user session
/*
 INSERT INTO USER_SESSION_LOG (
 session_id,
 user_name,
 user_email,
 session_start,
 pages_visited
 )
 VALUES (
 'session_' || UUID_STRING(),
 'test_user',
 'test@example.com',
 CURRENT_TIMESTAMP(),
 ARRAY_CONSTRUCT('Home', 'Dashboard')
 );
 */
-- Test the log action procedure
-- CALL SP_LOG_ACTION('TEST_ACTION', 'test_user', 'Test Org', 'Test Loc', 'Test Item', OBJECT_CONSTRUCT('test', 'data'), 'test_session_123');
-- Test the create order procedure
-- CALL SP_CREATE_ORDER('City Hospital', 'Emergency Unit', 'Paracetamol', 100, 'test_user', 'HIGH');
-- ============================================
-- PERFORMANCE MONITORING
-- ============================================
-- Check insert performance (should be <100ms)
/*
 SELECT 
 AVG(response_time_ms) as avg_response_ms,
 MIN(response_time_ms) as min_response_ms,
 MAX(response_time_ms) as max_response_ms,
 COUNT(*) as total_actions
 FROM ACTION_LOG
 WHERE action_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP());
 */
-- ============================================
-- CLEANUP COMMANDS
-- ============================================
-- Drop hybrid tables (if needed)
-- DROP HYBRID TABLE IF EXISTS ORDER_TRACKING;
-- DROP HYBRID TABLE IF EXISTS USER_SESSION_LOG;
-- DROP HYBRID TABLE IF EXISTS ACTION_LOG;
-- Drop views
-- DROP VIEW IF EXISTS VW_PENDING_ORDERS;
-- DROP VIEW IF EXISTS VW_ACTIVE_SESSIONS;
-- DROP VIEW IF EXISTS VW_RECENT_ACTIONS;
-- ============================================
-- NOTES FOR DEPLOYMENT
-- ============================================
-- 1. Unistore/Hybrid tables require Enterprise Edition or higher
-- 2. Different pricing model than standard tables
-- 3. Best for high-frequency transactional workloads
-- 4. Primary keys are enforced (unlike standard tables)
-- 5. Supports row-level locking for concurrent writes
-- 6. Optimized for single-row lookups and updates
-- 7. Can be queried together with standard tables in same query