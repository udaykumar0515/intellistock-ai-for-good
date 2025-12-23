"""
Snowflake Connector for IntelliStock
Uses password-based authentication via environment variables.
"""

import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

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
    Simple connection test.
    """
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
        )
        return True
    finally:
        cur.close()
        conn.close()
