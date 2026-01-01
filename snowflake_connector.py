"""
Snowflake Connector for IntelliStock
Flexible config loading:
 - Loads `.env` locally via python-dotenv if available
 - Reads env vars (after .env is loaded)
 - Falls back to Streamlit `st.secrets` when env vars are missing
This makes it safe to deploy with Streamlit Cloud secrets and to run locally with a `.env` file.
"""

import os
import snowflake.connector
import pandas as pd

# Try to load `.env` if python-dotenv is installed. This is non-fatal if not present.
try:
    from dotenv import load_dotenv
    load_dotenv()  # loads .env into environment variables if file exists
except Exception:
    pass

# Try to import streamlit (optional). If available, we'll read `st.secrets` as a fallback.
try:
    import streamlit as st
except Exception:
    st = None


def _get_st_secret(key: str):
    """Return secret value from Streamlit secrets if available.

    Supports both top-level keys (SNOWFLAKE_USER) and a nested `snowflake` section
    (e.g., secrets.toml with `[snowflake] SNOWFLAKE_USER = "..."`).
    """
    if not st:
        return None
    try:
        # Try top-level secret key first
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        # st.secrets may behave like an attribute in special contexts; ignore errors
        pass

    try:
        nested = st.secrets.get("snowflake")
        if isinstance(nested, dict):
            return nested.get(key)
    except Exception:
        pass

    return None


def _get_env_or_secret(key: str):
    """Return value from env var if present, else from Streamlit secrets if present."""
    val = os.getenv(key)
    if val:
        return val
    return _get_st_secret(key)


def get_snowflake_config() -> dict:
    """Return a dict with Snowflake connection parameters, or raise RuntimeError if missing."""
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]

    cfg = {k: _get_env_or_secret(k) for k in required_vars}
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise RuntimeError(f"Missing Snowflake config values: {', '.join(missing)}")

    return cfg


def get_snowflake_connection():
    """Create and return a Snowflake connection using combined config sources."""
    cfg = get_snowflake_config()
    return snowflake.connector.connect(
        account=cfg["SNOWFLAKE_ACCOUNT"],
        user=cfg["SNOWFLAKE_USER"],
        password=cfg["SNOWFLAKE_PASSWORD"],
        role=cfg["SNOWFLAKE_ROLE"],
        warehouse=cfg["SNOWFLAKE_WAREHOUSE"],
        database=cfg["SNOWFLAKE_DATABASE"],
        schema=cfg["SNOWFLAKE_SCHEMA"],
    )


def execute_query(query: str) -> pd.DataFrame:
    """
    Execute a SQL query and return results as DataFrame.
    """
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
