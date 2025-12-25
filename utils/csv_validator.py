"""
CSV Validation Module for IntelliStock
Validates uploaded inventory CSV files against required schema
"""

import pandas as pd
from datetime import datetime

# Required columns definition
REQUIRED_COLUMNS = [
    ("date", "DATE (YYYY-MM-DD)", "Transaction date", "2024-01-15"),
    ("organization", "STRING", "Organization name", "City Hospital"),
    ("location", "STRING", "Warehouse/clinic location", "Emergency Unit"),
    ("item", "STRING", "Product name", "Paracetamol"),
    ("opening_stock", "INTEGER", "Stock at start of day", "100"),
    ("received", "INTEGER", "Units received", "50"),
    ("issued", "INTEGER", "Units used/distributed", "30"),
    ("closing_stock", "INTEGER", "Stock at end of day", "120"),
    ("lead_time_days", "INTEGER", "Supplier delivery time", "7")
]

REQUIRED_COLUMN_NAMES = [col[0] for col in REQUIRED_COLUMNS]

def validate_inventory_csv(df):
    """
    Validate uploaded CSV against required schema.
    
    Returns:
        (is_valid, errors, warnings)
    """
    errors = []
    warnings = []
    
    # 1. Check required columns
    df_columns = [col.lower().strip() for col in df.columns]
    missing_columns = [col for col in REQUIRED_COLUMN_NAMES if col.lower() not in df_columns]
    
    if missing_columns:
        errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        return False, errors, warnings  # Cannot proceed without columns
    
    # Normalize column names (case-insensitive)
    df.columns = [col.lower().strip() for col in df.columns]
    
    # 2. Check for empty dataframe
    if len(df) == 0:
        errors.append("CSV file is empty (no data rows)")
        return False, errors, warnings
    
    # 3. Validate date column
    try:
        df['date'] = pd.to_datetime(df['date'])
    except Exception as e:
        errors.append(f"Invalid date format in 'date' column. Expected YYYY-MM-DD. Error: {str(e)}")
    
    # 4. Validate integer columns
    integer_columns = ['opening_stock', 'received', 'issued', 'closing_stock', 'lead_time_days']
    
    for col in integer_columns:
        try:
            # Check if convertible to numeric
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Check for NaN values after conversion
            if df[col].isna().any():
                nan_count = df[col].isna().sum()
                errors.append(f"Column '{col}' has {nan_count} non-numeric value(s)")
            
            # Check for negative values
            if (df[col] < 0).any():
                neg_count = (df[col] < 0).sum()
                errors.append(f"Column '{col}' has {neg_count} negative value(s) - stock cannot be negative")
        
        except Exception as e:
            errors.append(f"Error validating '{col}': {str(e)}")
    
    # 5. Check closing stock formula (warning only)
    try:
        df['calculated_closing'] = df['opening_stock'] + df['received'] - df['issued']
        mismatches = (df['closing_stock'] != df['calculated_closing']).sum()
        if mismatches > 0:
            warnings.append(
                f"{mismatches} row(s) where closing_stock ≠ opening_stock + received - issued. "
                "This may indicate data entry errors."
            )
    except:
        pass  # Skip if calculation fails
    
    # 6. Check for string columns
    string_columns = ['organization', 'location', 'item']
    for col in string_columns:
        if df[col].isna().any():
            na_count = df[col].isna().sum()
            errors.append(f"Column '{col}' has {na_count} empty value(s)")
    
    is_valid = len(errors) == 0
    
    return is_valid, errors, warnings
