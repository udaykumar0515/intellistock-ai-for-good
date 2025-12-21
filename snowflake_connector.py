"""
Snowflake Connector for IntelliStock
Manages connection to Snowflake using environment variables.
"""

import os
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_snowflake_connection():
    """
    Creates and returns a Snowflake connection using environment variables.
    Uses session state to maintain a single connection across Streamlit reruns.
    
    Returns:
        snowflake.connector.SnowflakeConnection: Active Snowflake connection
        
    Raises:
        ValueError: If required environment variables are missing
        Exception: If connection fails
    """
    # Check if connection already exists in session state
    if 'snowflake_conn' in st.session_state and st.session_state.snowflake_conn:
        try:
            # Test if connection is still active
            st.session_state.snowflake_conn.cursor().execute("SELECT 1")
            return st.session_state.snowflake_conn
        except:
            # Connection is stale, will create a new one
            st.session_state.snowflake_conn = None
    
    # Get required environment variables
    required_vars = {
        'account': 'SNOWFLAKE_ACCOUNT',
        'user': 'SNOWFLAKE_USER',
        'role': 'SNOWFLAKE_ROLE',
        'warehouse': 'SNOWFLAKE_WAREHOUSE',
        'database': 'SNOWFLAKE_DATABASE',
        'schema': 'SNOWFLAKE_SCHEMA'
    }
    
    config = {}
    missing_vars = []
    
    for key, env_var in required_vars.items():
        value = os.getenv(env_var)
        if not value:
            missing_vars.append(env_var)
        else:
            config[key] = value
    
    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}\n"
            f"Please create a .env file based on .env.example with your Snowflake credentials."
        )
    
    # Create connection with externalbrowser authentication
    try:
        conn = snowflake.connector.connect(
            account=config['account'],
            user=config['user'],
            role=config['role'],
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config['schema'],
            authenticator='externalbrowser'
        )
        
        # Store in session state
        st.session_state.snowflake_conn = conn
        return conn
        
    except Exception as e:
        raise Exception(f"Failed to connect to Snowflake: {str(e)}")


def execute_query(query, params=None):
    """
    Executes a SQL query and returns results as a pandas DataFrame.
    
    Args:
        query (str): SQL query to execute
        params (dict, optional): Query parameters for parameterized queries
        
    Returns:
        pandas.DataFrame: Query results
        
    Raises:
        Exception: If query execution fails
    """
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Fetch results and convert to DataFrame
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        
        import pandas as pd
        return pd.DataFrame(data, columns=columns)
        
    except Exception as e:
        raise Exception(f"Query execution failed: {str(e)}\nQuery: {query}")


def create_tables_if_not_exist():
    """
    Creates the required Snowflake database, schema, and tables if they don't exist.
    Uses idempotent DDL (CREATE IF NOT EXISTS).
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Create database
        cursor.execute("CREATE DATABASE IF NOT EXISTS INTELLISTOCK_DB")
        
        # Use database
        cursor.execute("USE DATABASE INTELLISTOCK_DB")
        
        # Create schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
        
        # Use schema
        cursor.execute("USE SCHEMA PUBLIC")
        
        # Create inventory table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS INVENTORY (
            date DATE,
            organization STRING,
            location STRING,
            item STRING,
            opening_stock INTEGER,
            received INTEGER,
            issued INTEGER,
            closing_stock INTEGER,
            lead_time_days INTEGER
        )
        """
        cursor.execute(create_table_sql)
        
        cursor.close()
        return True
        
    except Exception as e:
        st.error(f"Failed to create tables: {str(e)}")
        return False


def load_csv_data(csv_path):
    """
    Loads data from CSV file into Snowflake INVENTORY table.
    
    Args:
        csv_path (str): Path to CSV file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import pandas as pd
        
        # Read CSV
        df = pd.read_csv(csv_path)
        
        # Get connection
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Clear existing data (for development)
        cursor.execute("TRUNCATE TABLE IF EXISTS INVENTORY")
        
        # Insert data row by row
        insert_sql = """
        INSERT INTO INVENTORY (
            date, organization, location, item, 
            opening_stock, received, issued, closing_stock, lead_time_days
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for _, row in df.iterrows():
            cursor.execute(insert_sql, (
                row['date'],
                row['organization'],
                row['location'],
                row['item'],
                int(row['opening_stock']),
                int(row['received']),
                int(row['issued']),
                int(row['closing_stock']),
                int(row['lead_time_days'])
            ))
        
        conn.commit()
        cursor.close()
        return True
        
    except Exception as e:
        st.error(f"Failed to load CSV data: {str(e)}")
        return False


def test_connection():
    """
    Tests the Snowflake connection and displays status.
    """
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        result = cursor.fetchone()
        cursor.close()
        
        st.success("✅ Connected to Snowflake successfully!")
        st.info(f"User: {result[0]} | Role: {result[1]} | Database: {result[2]} | Schema: {result[3]}")
        return True
        
    except ValueError as e:
        st.error(f"❌ Configuration Error: {str(e)}")
        return False
        
    except Exception as e:
        st.error(f"❌ Connection Error: {str(e)}")
        return False


if __name__ == "__main__":
    """
    Standalone test script for Snowflake connection.
    Run this file directly to test your connection: python snowflake_connector.py
    """
    print("Testing Snowflake connection...")
    
    # Note: When running standalone, st.session_state won't work
    # Create a simple test without Streamlit
    load_dotenv()
    
    required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_ROLE', 
                     'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA']
    
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"❌ Missing environment variables: {', '.join(missing)}")
        print("Please create a .env file based on .env.example")
    else:
        try:
            conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                role=os.getenv('SNOWFLAKE_ROLE'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA'),
                authenticator='externalbrowser'
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            result = cursor.fetchone()
            cursor.close()
            
            print("✅ Connection successful!")
            print(f"User: {result[0]}")
            print(f"Role: {result[1]}")
            print(f"Database: {result[2]}")
            print(f"Schema: {result[3]}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
