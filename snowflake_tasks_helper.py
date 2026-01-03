"""
Snowflake Tasks and Streams Helper Module
==========================================
Provides helper functions for managing Snowflake tasks, streams,
dynamic tables, and Unistore operations from the Streamlit application.

Functions:
- Task management (resume, suspend, status, history)
- Stream monitoring (status, offset, data checks)
- Dynamic table operations (refresh, status)
- Unistore action logging
- Performance monitoring
"""

import os
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime, timedelta

try:
    import streamlit as st
except ImportError:
    st = None

# Import from existing connector
from snowflake_connector import execute_query, get_snowflake_connection


# ============================================
# TASK MANAGEMENT FUNCTIONS
# ============================================

def get_task_status(task_name: str) -> Dict[str, Any]:
    """
    Get detailed status of a Snowflake task.
    
    Args:
        task_name: Name of the task
        
    Returns:
        Dictionary with task status information
    """
    try:
        query = f"SHOW TASKS LIKE '{task_name}'"
        result = execute_query(query)
        
        if result.empty:
            return {"error": f"Task {task_name} not found"}
        
        task_info = result.iloc[0]
        return {
            "name": task_info.get("name", task_name),
            "state": task_info.get("state", "UNKNOWN"),
            "schedule": task_info.get("schedule", "N/A"),
            "warehouse": task_info.get("warehouse", "N/A"),
            "predecessor": task_info.get("predecessors", "None"),
            "condition": task_info.get("condition", "N/A"),
            "created_on": task_info.get("created_on", "N/A")
        }
    except Exception as e:
        return {"error": str(e)}


def resume_task(task_name: str) -> bool:
    """
    Resume (start) a suspended task.
    
    Args:
        task_name: Name of the task to resume
        
    Returns:
        True if successful, False otherwise
    """
    try:
        query = f"ALTER TASK {task_name} RESUME"
        execute_query(query)
        return True
    except Exception as e:
        print(f"Error resuming task {task_name}: {e}")
        return False


def suspend_task(task_name: str) -> bool:
    """
    Suspend (pause) a running task.
    
    Args:
        task_name: Name of the task to suspend
        
    Returns:
        True if successful, False otherwise
    """
    try:
        query = f"ALTER TASK {task_name} SUSPEND"
        execute_query(query)
        return True
    except Exception as e:
        print(f"Error suspending task {task_name}: {e}")
        return False


def get_task_execution_history(task_name: str, hours: int = 24) -> pd.DataFrame:
    """
    Get execution history for a task.
    
    Args:
        task_name: Name of the task
        hours: Number of hours of history to retrieve
        
    Returns:
        DataFrame with task execution history
    """
    try:
        query = f"""
        SELECT 
            name,
            state,
            scheduled_time,
            completed_time,
            return_value,
            error_code,
            error_message
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
            TASK_NAME => '{task_name}',
            SCHEDULED_TIME_RANGE_START => DATEADD('hour', -{hours}, CURRENT_TIMESTAMP())
        ))
        ORDER BY scheduled_time DESC
        LIMIT 100
        """
        return execute_query(query)
    except Exception as e:
        print(f"Error getting task history: {e}")
        return pd.DataFrame()


def execute_task_manually(task_name: str) -> bool:
    """
    Manually execute a task (for testing).
    
    Args:
        task_name: Name of the task to execute
        
    Returns:
        True if successful, False otherwise
    """
    try:
        query = f"EXECUTE TASK {task_name}"
        execute_query(query)
        return True
    except Exception as e:
        print(f"Error executing task {task_name}: {e}")
        return False


# ============================================
# STREAM MANAGEMENT FUNCTIONS
# ============================================

def get_stream_status(stream_name: str) -> Dict[str, Any]:
    """
    Get status of a Snowflake stream.
    
    Args:
        stream_name: Name of the stream
        
    Returns:
        Dictionary with stream status information
    """
    try:
        query = f"SHOW STREAMS LIKE '{stream_name}'"
        result = execute_query(query)
        
        if result.empty:
            return {"error": f"Stream {stream_name} not found"}
        
        stream_info = result.iloc[0]
        return {
            "name": stream_info.get("name", stream_name),
            "table_name": stream_info.get("table_name", "N/A"),
            "stale": stream_info.get("stale", "N/A"),
            "mode": stream_info.get("mode", "N/A"),
            "created_on": stream_info.get("created_on", "N/A")
        }
    except Exception as e:
        return {"error": str(e)}


def stream_has_data(stream_name: str) -> bool:
    """
    Check if a stream has pending data.
    
    Args:
        stream_name: Name of the stream
        
    Returns:
        True if stream has data, False otherwise
    """
    try:
        query = f"SELECT SYSTEM$STREAM_HAS_DATA('{stream_name}') as has_data"
        result = execute_query(query)
        return bool(result.iloc[0]['HAS_DATA']) if not result.empty else False
    except Exception as e:
        print(f"Error checking stream data: {e}")
        return False


def get_stream_changes_count(stream_name: str) -> int:
    """
    Get count of pending changes in a stream.
    
    Args:
        stream_name: Name of the stream
        
    Returns:
        Number of pending changes
    """
    try:
        query = f"SELECT COUNT(*) as change_count FROM {stream_name}"
        result = execute_query(query)
        return int(result.iloc[0]['CHANGE_COUNT']) if not result.empty else 0
    except Exception as e:
        print(f"Error getting stream changes: {e}")
        return 0


# ============================================
# DYNAMIC TABLE MANAGEMENT FUNCTIONS
# ============================================

def get_dynamic_table_refresh_status(table_name: str) -> Dict[str, Any]:
    """
    Get refresh status of a dynamic table.
    
    Args:
        table_name: Name of the dynamic table
        
    Returns:
        Dictionary with refresh status information
    """
    try:
        query = f"""
        SELECT 
            name,
            state,
            target_lag,
            data_timestamp,
            refresh_mode,
            last_refresh_start_time,
            last_refresh_end_time
        FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
            NAME => '{table_name}'
        ))
        ORDER BY last_refresh_start_time DESC
        LIMIT 1
        """
        result = execute_query(query)
        
        if result.empty:
            return {"error": f"No refresh history found for {table_name}"}
        
        refresh_info = result.iloc[0]
        return {
            "name": refresh_info.get("NAME", table_name),
            "state": refresh_info.get("STATE", "UNKNOWN"),
            "target_lag": refresh_info.get("TARGET_LAG", "N/A"),
            "data_timestamp": refresh_info.get("DATA_TIMESTAMP", "N/A"),
            "last_refresh_start": refresh_info.get("LAST_REFRESH_START_TIME", "N/A"),
            "last_refresh_end": refresh_info.get("LAST_REFRESH_END_TIME", "N/A")
        }
    except Exception as e:
        return {"error": str(e)}


def manual_refresh_dynamic_table(table_name: str) -> bool:
    """
    Manually trigger refresh of a dynamic table.
    
    Args:
        table_name: Name of the dynamic table
        
    Returns:
        True if successful, False otherwise
    """
    try:
        query = f"ALTER DYNAMIC TABLE {table_name} REFRESH"
        execute_query(query)
        return True
    except Exception as e:
        print(f"Error refreshing dynamic table {table_name}: {e}")
        return False


def get_dynamic_table_last_updated(table_name: str) -> Optional[datetime]:
    """
    Get the last update timestamp for a dynamic table.
    
    Args:
        table_name: Name of the dynamic table
        
    Returns:
        datetime of last update, or None if unavailable
    """
    try:
        status = get_dynamic_table_refresh_status(table_name)
        if "last_refresh_end" in status and status["last_refresh_end"] != "N/A":
            return status["last_refresh_end"]
        return None
    except Exception:
        return None


# ============================================
# UNISTORE ACTION LOGGING FUNCTIONS
# ============================================

def log_action_to_unistore(
    action_type: str,
    user_name: str = None,
    organization: str = None,
    location: str = None,
    item: str = None,
    details: Dict = None,
    session_id: str = None
) -> Optional[int]:
    """
    Log an action to the Unistore ACTION_LOG hybrid table.
    
    Args:
        action_type: Type of action ('ORDER_PLACED', 'PDF_EXPORTED', etc.)
        user_name: User performing the action
        organization: Organization name (if applicable)
        location: Location name (if applicable)
        item: Item name (if applicable)
        details: Additional details as dictionary
        session_id: Session ID for tracking
        
    Returns:
        action_id if successful, None otherwise
    """
    try:
        # Get current user from Snowflake session if not provided
        if user_name is None:
            user_result = execute_query("SELECT CURRENT_USER() as user")
            user_name = user_result.iloc[0]['USER'] if not user_result.empty else 'unknown'
        
        # Get session ID from Streamlit if available and not provided
        if session_id is None and st is not None:
            try:
                session_id = st.session_state.get('session_id', 'unknown')
            except:
                session_id = 'unknown'
        
        # Convert details dict to JSON string
        details_json = None
        if details:
            import json
            details_json = json.dumps(details)
        
        # Use stored procedure for cleanerinsert
        query = f"""
        CALL SP_LOG_ACTION(
            '{action_type}',
            '{user_name}',
            {f"'{organization}'" if organization else 'NULL'},
            {f"'{location}'" if location else 'NULL'},
            {f"'{item}'" if item else 'NULL'},
            {f"PARSE_JSON('{details_json}')" if details_json else 'NULL'},
            '{session_id}'
        )
        """
        execute_query(query)
        
        # Get the last inserted action_id
        result = execute_query("SELECT MAX(action_id) as last_id FROM ACTION_LOG")
        return int(result.iloc[0]['LAST_ID']) if not result.empty else None
        
    except Exception as e:
        print(f"Error logging action to Unistore: {e}")
        return None


def create_order_in_unistore(
    organization: str,
    location: str,
    item: str,
    ordered_qty: float,
    user_name: str = None,
    priority: str = 'MEDIUM'
) -> Optional[int]:
    """
    Create an order entry in Unistore ORDER_TRACKING table.
    
    Args:
        organization: Organization name
        location: Location name
        item: Item name
        ordered_qty: Quantity ordered
        user_name: User placing the order
        priority: Priority level ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW')
        
    Returns:
        order_id if successful, None otherwise
    """
    try:
        if user_name is None:
            user_result = execute_query("SELECT CURRENT_USER() as user")
            user_name = user_result.iloc[0]['USER'] if not user_result.empty else 'unknown'
        
        query = f"""
        CALL SP_CREATE_ORDER(
            '{organization}',
            '{location}',
            '{item}',
            {ordered_qty},
            '{user_name}',
            '{priority}'
        )
        """
        result = execute_query(query)
        
        # Extract order ID from result message
        if not result.empty:
            message = str(result.iloc[0][0])
            if "Order ID:" in message:
                order_id = int(message.split("Order ID:")[-1].strip())
                return order_id
        
        return None
        
    except Exception as e:
        print(f"Error creating order in Unistore: {e}")
        return None


def get_recent_actions(hours: int = 24, limit: int = 100) -> pd.DataFrame:
    """
    Get recent actions from ACTION_LOG.
    
    Args:
        hours: Number of hours to look back
        limit: Maximum number of records to return
        
    Returns:
        DataFrame with recent actions
    """
    try:
        query = f"""
        SELECT 
            action_id,
            action_type,
            action_timestamp,
            user_name,
            organization,
            location,
            item,
            status
        FROM ACTION_LOG
        WHERE action_timestamp >= DATEADD('hour', -{hours}, CURRENT_TIMESTAMP())
        ORDER BY action_timestamp DESC
        LIMIT {limit}
        """
        return execute_query(query)
    except Exception as e:
        print(f"Error getting recent actions: {e}")
        return pd.DataFrame()


# ============================================
# MONITORING AND ANALYTICS FUNCTIONS
# ============================================

def get_system_health_dashboard() -> Dict[str, Any]:
    """
    Get overall health status of all Snowflake components.
    
    Returns:
        Dictionary with health metrics for all components
    """
    health = {
        "tasks": {},
        "streams": {},
        "dynamic_tables": {},
        "timestamp": datetime.now().isoformat()
    }
    
    # Check tasks
    task_names = [
        'TASK_REFRESH_ANALYTICS_SUMMARY',
        'TASK_GENERATE_ALERTS',
        'TASK_DAILY_CLEANUP'
    ]
    
    for task_name in task_names:
        status = get_task_status(task_name)
        health["tasks"][task_name] = {
            "state": status.get("state", "UNKNOWN"),
            "healthy": status.get("state") == "started"
        }
    
    # Check streams
    stream_names = ['INVENTORY_CHANGES', 'ANALYTICS_CHANGES']
    
    for stream_name in stream_names:
        has_data = stream_has_data(stream_name)
        changes = get_stream_changes_count(stream_name)
        health["streams"][stream_name] = {
            "has_data": has_data,
            "pending_changes": changes,
            "healthy": True  # Streams are always healthy if they exist
        }
    
    # Check dynamic tables
    dt_names = [
        'STOCK_ANALYTICS_DT',
        'REORDER_RECOMMENDATIONS_DT',
        'DAILY_USAGE_STATS_DT'
    ]
    
    for dt_name in dt_names:
        status = get_dynamic_table_refresh_status(dt_name)
        health["dynamic_tables"][dt_name] = {
            "state": status.get("state", "UNKNOWN"),
            "last_refresh": str(status.get("last_refresh_end", "N/A")),
            "healthy": status.get("state") in ["READY", "RUNNING"]
        }
    
    return health


def get_performance_metrics() -> Dict[str, Any]:
    """
    Get performance metrics for the system.
    
    Returns:
        Dictionary with performance metrics
    """
    metrics = {}
    
    try:
        # Task execution performance
        query = """
        SELECT 
            task_name,
            AVG(execution_duration_seconds) as avg_duration,
            MAX(execution_duration_seconds) as max_duration,
            COUNT(*) as executions,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful
        FROM TASK_EXECUTION_LOG
        WHERE execution_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        GROUP BY task_name
        """
        metrics["task_performance"] = execute_query(query).to_dict('records')
        
        # Action log statistics
        query = """
        SELECT 
            action_type,
            COUNT(*) as count,
            AVG(response_time_ms) as avg_response_ms
        FROM ACTION_LOG
        WHERE action_timestamp >= DATEADD('day', -1, CURRENT_TIMESTAMP())
        GROUP BY action_type
        """
        metrics["action_stats"] = execute_query(query).to_dict('records')
        
    except Exception as e:
        print(f"Error getting performance metrics: {e}")
    
    return metrics


# ============================================
# UTILITY FUNCTIONS
# ============================================

def initialize_session_in_unistore(user_name: str = None) -> str:
    """
    Initialize a new user session in Unistore.
    
    Args:
        user_name: User name (optional)
        
    Returns:
        session_id
    """
    try:
        import uuid
        session_id = f"session_{uuid.uuid4().hex[:12]}"
        
        if user_name is None:
            user_result = execute_query("SELECT CURRENT_USER() as user")
            user_name = user_result.iloc[0]['USER'] if not user_result.empty else 'unknown'
        
        query = f"""
        INSERT INTO USER_SESSION_LOG (
            session_id,
            user_name,
            session_start,
            is_active
        )
        VALUES (
            '{session_id}',
            '{user_name}',
            CURRENT_TIMESTAMP(),
            TRUE
        )
        """
        execute_query(query)
        
        return session_id
        
    except Exception as e:
        print(f"Error initializing session: {e}")
        return "unknown"


def close_session_in_unistore(session_id: str):
    """
    Close a user session in Unistore.
    
    Args:
        session_id: Session ID to close
    """
    try:
        query = f"""
        UPDATE USER_SESSION_LOG
        SET is_active = FALSE,
            session_end = CURRENT_TIMESTAMP(),
            session_duration_minutes = DATEDIFF('minute', session_start, CURRENT_TIMESTAMP())
        WHERE session_id = '{session_id}'
        """
        execute_query(query)
    except Exception as e:
        print(f"Error closing session: {e}")


if __name__ == "__main__":
    # Test functions
    print("Testing Snowflake Tasks & Streams Helper...")
    
    # Test task status
    print("\n1. Task Status:")
    status = get_task_status('TASK_REFRESH_ANALYTICS_SUMMARY')
    print(status)
    
    # Test stream status
    print("\n2. Stream Status:")
    stream_status = get_stream_status('INVENTORY_CHANGES')
    print(stream_status)
    
    # Test dynamic table status
    print("\n3. Dynamic Table Status:")
    dt_status = get_dynamic_table_refresh_status('STOCK_ANALYTICS_DT')
    print(dt_status)
    
    # Test system health
    print("\n4. System Health:")
    health = get_system_health_dashboard()
    print(health)
    
    print("\nAll tests completed!")
