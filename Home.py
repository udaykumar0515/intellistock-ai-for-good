"""
IntelliStock v2.1 - Home Page
Multi-page Streamlit application for inventory management
"""

import streamlit as st

# Page configuration
st.set_page_config(
    page_title="IntelliStock - Home",
    page_icon="📦",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        text-align: center;
        color: #555;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-header">📦 IntelliStock v2.1</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">AI-Driven Inventory Health & Stock-Out Alert System</div>', unsafe_allow_html=True)

st.markdown("---")

# Welcome section
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 📊 Dashboard")
    st.markdown("""
    View real-time analytics and make data-driven decisions:
    - Today's top priority actions
    - Stock-out alerts with trends
    - What-If order calculator
    - Mark items as ordered
    - Export reports to PDF
    """)
    st.page_link("pages/1__Dashboard.py", label="Go to Dashboard →", icon="📊")

with col2:
    st.markdown("### 📁 Data Management")
    st.markdown("""
    Upload and manage your inventory data:
    - Upload custom CSV files
    - Validate data quality
    - Preview before loading
    - Database management tools
    """)
    st.page_link("pages/2__Data_Management.py", label="Go to Data Management →", icon="📁")

with col3:
    st.markdown("### ⚙️ Configuration")
    st.markdown("""
    Customize priority scoring rules:
    - Location criticality scores
    - Item criticality scores
    - Default scoring values
    - Save and reload settings
    """)
    st.page_link("pages/3__Configuration.py", label="Go to Configuration →", icon="⚙️")

st.markdown("---")

# Features overview
st.markdown("## ✨ Key Features")

features_col1, features_col2 = st.columns(2)

with features_col1:
    st.markdown("""
    **🎯 Decision Support:**
    - Priority-ranked action panel
    - Risk-based alert system
    - 7-day trend visualization (sparklines)
    - Interactive order projections
    
    **📈 Analytics:**
    - Real-time inventory metrics
    - Location-based heatmaps
    - Usage pattern analysis
    - Lead time tracking
    """)

with features_col2:
    st.markdown("""
    **🔧 Customization:**
    - Configurable criticality rules
    - Location-specific weights
    - Item-specific priorities
    - JSON-based configuration
    
    **📁 Data Management:**
    - CSV file upload
    - Schema validation
    - Snowflake integration
    - Organization profiles
    """)

st.markdown("---")

# Quick start guide
with st.expander("🚀 Quick Start Guide"):
    st.markdown("""
    ### First Time Setup:
    
    1. **Test Connection** - Go to Data Management and test your Snowflake connection
    2. **Initialize Database** - Create required tables in Snowflake
    3. **Upload Data** - Upload your inventory CSV file (or use sample data)
    4. **Set Organization Profile** - Fill in your organization details
    5. **View Dashboard** - Navigate to Dashboard to see analytics
    6. **Configure Scoring** - Optionally adjust criticality rules in Configuration
    
    ### Daily Usage:
    
    1. **Check Action Panel** - See top 3 priority items to reorder
    2. **Review Alerts** - Identify all high-risk stock-outs
    3. **Use What-If Calculator** - Project impact of ordering
    4. **Mark as Ordered** - Track what you've already ordered
    5. **Export PDF** - Download action items for sharing
    """)

st.info("👈 Use the sidebar to navigate between pages")

# Footer
st.markdown("---")
st.caption("IntelliStock v2.1 | Built with Streamlit &  Snowflake | AI for Good Hackathon")
