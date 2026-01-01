"""
Snowflake Connector for IntelliStock
Uses password-based authentication via environment variables.
"""

import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.context import set_active_session, get_active_session as sp_get_active_session

# Load environment variables
load_dotenv()


def get_snowflake_connection():
    """
    Create and return a Snowflake connection using env variables.
    """
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA"
    ]

    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        raise RuntimeError(
            f"Missing environment variables: {', '.join(missing)}"
        )

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def create_snowpark_session(config: dict = None) -> Session:
    """
    Create and return a Snowpark Session. Reads configuration from ENV if no config provided.
    Also sets the session as the active session (so Streamlit's get_active_session() works).
    """
    if config is None:
        # Build config from environment variables (dotenv should be loaded already)
        config = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        }
        # Optionally support external browser authenticator
        if os.getenv('SNOWFLAKE_AUTHENTICATOR'):
            config['authenticator'] = os.getenv('SNOWFLAKE_AUTHENTICATOR')

    # Remove None values
    config = {k: v for k, v in config.items() if v is not None}

    session = Session.builder.configs(config).create()
    # Set as active so get_active_session() works in other modules
    try:
        set_active_session(session)
    except Exception:
        # Non-critical if set_active_session fails
        pass
    return session


def get_snowpark_session() -> Session:
    """Return an active Snowpark session if available, otherwise create one."""
    try:
        s = sp_get_active_session()
        if s is not None:
            return s
    except Exception:
        pass
    # Create a new one if not found
    try:
        return create_snowpark_session()
    except Exception as e:
        # Could not create Snowpark session
        raise RuntimeError(f"Could not create Snowpark session: {e}")


def execute_query(query: str) -> pd.DataFrame:
    """
    Execute a SQL query and return results as DataFrame.
    Prefer using Snowpark session when available, otherwise fall back to connector.
    """
    # Try using Snowpark first
    try:
        session = get_snowpark_session()
        if session is not None:
            return session.sql(query).to_pandas()
    except Exception:
        # Fall back to connector approach on any failure
        pass

    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute(query)
        df = cur.fetch_pandas_all()
        return df
    finally:
        cur.close()
        conn.close()


def create_tables_if_not_exist():
    """
    Creates the required Snowflake database, schema, and tables if they don't exist.
    """
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        
        # Create database
        cur.execute("CREATE DATABASE IF NOT EXISTS INTELLISTOCK_DB")
        cur.execute("USE DATABASE INTELLISTOCK_DB")
        
        # Create schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS PUBLIC")
        cur.execute("USE SCHEMA PUBLIC")
        
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
        cur.execute(create_table_sql)
        return True
    except Exception as e:
        print(f"Error creating tables: {e}")
        return False
    finally:
        cur.close()
        conn.close()


def load_csv_data(csv_path):
    """
    Loads data from CSV file into Snowflake INVENTORY table.
    """
    try:
        # Read CSV
        df = pd.read_csv(csv_path)
        
        # Validate data
        if (df['closing_stock'] < 0).any():
            print("Warning: Found negative closing_stock values")
        
        # Get connection
        conn = get_snowflake_connection()
        cur = conn.cursor()
        
        # Clear existing data
        cur.execute("TRUNCATE TABLE IF EXISTS INVENTORY")
        
        # Insert data
        insert_sql = """
        INSERT INTO INVENTORY (
            date, organization, location, item, 
            opening_stock, received, issued, closing_stock, lead_time_days
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for _, row in df.iterrows():
            cur.execute(insert_sql, (
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
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return False


def test_connection() -> bool:
    """
    Test Snowflake connection and return True/False.
    Returns False on any connection error instead of raising exception.
    """
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
        )
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False
