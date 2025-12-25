"""
IntelliStock v2.1 - Data Management Page
CSV upload, validation, and database management
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
from snowflake_connector import execute_query, load_csv_data, create_tables_if_not_exist, test_connection
from utils.csv_validator import validate_inventory_csv, REQUIRED_COLUMNS
import os

st.set_page_config(page_title="Data Management - IntelliStock", page_icon="📁", layout="wide")

st.title("📁 Data Management")
st.markdown("Upload and manage your inventory data. All uploads are validated before loading into Snowflake.")

st.markdown("---")

# =========================================
# Section 1: Required Schema Information
# =========================================
st.header("📋 Required CSV Schema")

st.markdown("""
Your CSV file **must** contain these columns (case-insens▮itive):
""")

schema_df = pd.DataFrame(REQUIRED_COLUMNS, columns=[
    "Column Name", "Data Type", "Description", "Example"
])
st.table(schema_df)

st.info("💡 **Tip:** Download the sample CSV from `data/inventory_sample.csv` as a template")

st.markdown("---")

# =========================================
# Section 2: Upload CSV File
# =========================================
st.header("📤 Upload Inventory Data")

uploaded_file = st.file_uploader(
    "Choose a CSV file",
    type=['csv'],
    help="Upload your inventory CSV file with the required schema"
)

if uploaded_file is not None:
    try:
        # Read CSV
        df = pd.read_csv(uploaded_file)
        
        st.success(f"✅ File uploaded: {uploaded_file.name} ({len(df)} rows)")
        
        # Validate
        is_valid, errors, warnings = validate_inventory_csv(df)
        
        # Display validation results
        st.markdown("---")
        st.header("🔍 Validation Results")
        
        if errors:
            st.error(f"❌ **{len(errors)} Validation Error(s) Found:**")
            for error in errors:
                st.error(f"• {error}")
        
        if warnings:
            st.warning(f"⚠️ **{len(warnings)} Warning(s):**")
            for warning in warnings:
                st.warning(f"• {warning}")
        
        if is_valid:
            st.success("✅ **All validations passed!** Data is ready to load.")
        
        # Data Preview
        st.markdown("---")
        st.header("👁️ Data Preview")
        st.caption(f"Showing first 20 rows of {len(df)} total rows")
        st.dataframe(df.head(20), use_container_width=True)
        
        # Load button (only if valid)
        if is_valid:
            st.markdown("---")
            st.header("💾 Load into Snowflake")
            
            st.warning("""
            ⚠️ **Warning:** This will replace all existing inventory data in Snowflake.
            Make sure you have a backup if needed.
            """)
            
            confirm = st.checkbox("I understand and want to proceed")
            
            if confirm:
                if st.button("🚀 Load Data into Snowflake", type="primary"):
                    with st.spinner("Loading data into Snowflake..."):
                        # Save to temp file
                        temp_path = Path("temp_upload.csv")
                        df.to_csv(temp_path, index=False)
                        
                        # Load using existing function
                        if load_csv_data(str(temp_path)):
                            temp_path.unlink()  # Delete temp file
                            st.success("✅ Data loaded successfully!")
                            
                            # Trigger organization metadata capture
                            st.session_state.show_org_form = True
                            st.rerun()
                        else:
                            st.error("❌ Failed to load data")
        
    except Exception as e:
        st.error(f"❌ Error reading CSV file: {str(e)}")

# =========================================
# Section 3: Organization Metadata (after successful upload)
# =========================================
if st.session_state.get('show_org_form', False):
    st.markdown("---")
    st.header("🏢 Organization Profile")
    
    st.markdown("Please provide your organization details:")
    
    with st.form("org_metadata_form"):
        org_name = st.text_input("Organization Name *")
        sector = st.selectbox(
            "Sector *",
            ["Hospital", "NGO", "Government", "Private", "Other"]
        )
        region = st.text_input("Country/State *")
        goods_type = st.text_input(
            "Type of Essential Goods Managed *",
            placeholder="e.g., Medical Supplies, Food Items, Equipment"
        )
        contact_email = st.text_input("Contact Email (optional)")
        
        submitted = st.form_submit_button("💾 Save Organization Profile")
        
        if submitted:
            if org_name and sector and region and goods_type:
                # Insert into metadata table
                try:
                    # Create table if not exists
                    create_query = """
                    CREATE TABLE IF NOT EXISTS ORG_METADATA (
                        organization STRING,
                        sector STRING,
                        region STRING,
                        goods_type STRING,
                        contact_email STRING,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                    )
                    """
                    execute_query(create_query)
                    
                    # Insert metadata
                    insert_query = f"""
                    INSERT INTO ORG_METADATA (organization, sector, region, goods_type, contact_email)
                    VALUES (
                        '{org_name.replace("'", "''")}',
                        '{sector}',
                        '{region.replace("'", "''")}',
                        '{goods_type.replace("'", "''")}',
                        '{contact_email.replace("'", "''") if contact_email else ""}'
                    )
                    """
                    execute_query(insert_query)
                    
                    st.success("✅ Organization profile saved!")
                    st.session_state.show_org_form = False
                    st.info("🎉 Data upload complete! Go to the Dashboard to view analytics.")
                    
                except Exception as e:
                    st.error(f"❌ Error saving profile: {str(e)}")
            else:
                st.error("❌ Please fill in all required fields")

st.markdown("---")

# =========================================
# Section 4: Database Management
# =========================================
st.header("🗄️ Database Management")

col1, col2, col3 = st.columns(3)

with col1:
    if st.button("🧪 Test Snowflake Connection"):
        if test_connection():
            st.success("✅ Connection successful!")
            st.info(f"📊 Database: {os.getenv('SNOWFLAKE_DATABASE')}\n\n🏢 Warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
        else:
            st.error("❌ Connection failed")

with col2:
    if st.button("🔧 Initialize Database Tables"):
        if create_tables_if_not_exist():
            st.success("✅ Tables created successfully!")
        else:
            st.error("❌ Failed to create tables")

with col3:
    if st.button("📊 Load Sample Data"):
        with st.spinner("Loading sample CSV..."):
            if load_csv_data("data/inventory_sample.csv"):
                st.success("✅ Sample data loaded!")
                st.info("📊 Go to Dashboard to view the data")
            else:
                st.error("❌ Failed to load sample data")

st.markdown("---")
st.caption("IntelliStock v2.1 | Data Management")
