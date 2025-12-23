"""
IntelliStock - AI-Driven Inventory Health & Stock-Out Alert System
Streamlit application for Snowflake AI for Good Hackathon
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from snowflake_connector import (
    get_snowflake_connection,
    execute_query,
    create_tables_if_not_exist,
    load_csv_data,
    test_connection
)
from utils.calculations import generate_explanation, get_urgency_level

# Page configuration
st.set_page_config(
    page_title="IntelliStock - Inventory Health Dashboard",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 0.5rem;
        text-align: center;
    }
    .alert-high {
        background-color: #fee;
        padding: 0.5rem;
        border-left: 4px solid #f44;
    }
    .alert-critical {
        background-color: #fdd;
        padding: 0.5rem;
        border-left: 4px solid #d00;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.markdown('<div class="main-header">📦 IntelliStock</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">AI-Driven Inventory Health & Stock-Out Alert System</div>', unsafe_allow_html=True)

# Helper function for priority scoring
def calculate_priority_score(row):
    """
    Calculate action priority score for high-risk items.
    Higher score = more urgent action required.
    
    Formula: (lead_time * 2) + (avg_usage * 1.5) + criticality - (stock * 0.5)
    """
    location = row.get('LOCATION', '')
    item = row.get('ITEM', '')
    
    # Criticality scoring based on location and item type
    if 'Emergency Unit' in location:
        criticality = 10
    elif item in ['Paracetamol', 'Insulin', 'Syringes', 'Bandages', 'Masks', 'Gloves']:
        criticality = 7
    elif item in ['Rice']:
        criticality = 5
    else:
        criticality = 3
    
    # Priority formula (deterministic, no ML)
    score = (
        (row['LEAD_TIME_DAYS'] * 2) +
        (row['AVG_DAILY_USAGE'] * 1.5) +
        criticality -
        (row['CLOSING_STOCK'] * 0.5)
    )
    return round(score, 2)

# Sidebar
with st.sidebar:
    st.markdown("---")
    
    st.header("🔧 Setup")
    
    # Connection test
    if st.button("Test Snowflake Connection"):
        test_connection()
    
    # Initialize database
    if st.button("Initialize Database"):
        with st.spinner("Creating tables..."):
            if create_tables_if_not_exist():
                st.success("✅ Database tables created successfully!")
            else:
                st.error("❌ Failed to create tables")
    
    # Load sample data
    if st.button("Load Sample Data"):
        with st.spinner("Loading CSV data..."):
            if load_csv_data("data/inventory_sample.csv"):
                st.success("✅ Sample data loaded successfully!")
                st.rerun()
            else:
                st.error("❌ Failed to load sample data")
    
    st.markdown("---")
    st.header("🔍 Filters")
    
    # Get filter options from data
    try:
        # Get distinct organizations
        org_query = "SELECT DISTINCT organization FROM INVENTORY ORDER BY organization"
        orgs = execute_query(org_query)
        org_list = ['All'] + orgs['ORGANIZATION'].tolist() if not orgs.empty else ['All']
        
        # Get distinct locations
        loc_query = "SELECT DISTINCT location FROM INVENTORY ORDER BY location"
        locs = execute_query(loc_query)
        loc_list = ['All'] + locs['LOCATION'].tolist() if not locs.empty else ['All']
        
        # Get distinct items
        item_query = "SELECT DISTINCT item FROM INVENTORY ORDER BY item"
        items = execute_query(item_query)
        item_list = ['All'] + items['ITEM'].tolist() if not items.empty else ['All']
        
    except Exception as e:
        st.warning(f"⚠️ Could not load filter options. Please initialize database and load data first.")
        org_list = ['All']
        loc_list = ['All']
        item_list = ['All']
    
    selected_org = st.selectbox("Organization", org_list)
    selected_loc = st.selectbox("Location", loc_list)
    selected_item = st.selectbox("Item", item_list)
    
    st.markdown("---")
    st.info("**Note:** Filters are for data slicing only. Analytics logic (thresholds, calculations) is defined in SQL and remains unchanged.")

# Main content
try:
    # =========================================
    # SECTION 0: Today's Action Panel
    # =========================================
    st.markdown("### 🎯 What You Should Do Today")
    st.caption("Top priority actions based on stock-out risk, usage patterns, and criticality")
    
    # Reuse same query structure as Stock-Out Alerts (no divergent logic)
    action_panel_query = f"""
    WITH analytics AS (
        SELECT 
            organization,
            location,
            item,
            closing_stock,
            lead_time_days,
            AVG(issued) OVER (PARTITION BY organization, location, item) as avg_daily_usage
        FROM INVENTORY
        WHERE 1=1 {where_str}
    ),
    risk_analysis AS (
        SELECT 
            organization,
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
        SELECT 
            *,
            CASE 
                WHEN days_left <= lead_time_days THEN 'HIGH'
                ELSE 'NORMAL'
            END as risk_status
        FROM risk_analysis
    )
    SELECT 
        organization,
        location,
        item,
        closing_stock,
        ROUND(avg_daily_usage, 2) as avg_daily_usage,
        ROUND(days_left, 2) as days_left,
        lead_time_days
    FROM risk_status
    WHERE risk_status = 'HIGH'
    """
    
    try:
        top_actions = execute_query(action_panel_query)
        
        if not top_actions.empty:
            # Calculate priority scores (same logic as alerts section)
            top_actions['PRIORITY_SCORE'] = top_actions.apply(calculate_priority_score, axis=1)
            top_actions = top_actions.sort_values('PRIORITY_SCORE', ascending=False).head(3)
            
            # Display each action with clear explanation
            for idx, row in top_actions.iterrows():
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(f"**{row['ITEM']}** • {row['ORGANIZATION']} – {row['LOCATION']}")
                    # Rule-based explanation (deterministic, no AI)
                    explanation = f"Reorder {row['ITEM']} at {row['ORGANIZATION']} – {row['LOCATION']}. High daily usage and long supplier lead time make this the most urgent action."
                    st.caption(explanation)
                with col2:
                    st.metric("Priority", f"{row['PRIORITY_SCORE']:.1f}")
        else:
            st.success("✅ No urgent actions needed today! All inventory levels are healthy.")
            
    except Exception as e:
        st.info("💡 Click 'Initialize Database' and 'Load Sample Data' to see action recommendations")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 1: Overview Metrics
    # =========================================
    st.header("📊 Overview Metrics")
    
    overview_query = """
    WITH analytics AS (
        SELECT 
            organization,
            location,
            item,
            closing_stock,
            lead_time_days,
            AVG(issued) OVER (PARTITION BY organization, location, item) as avg_daily_usage
        FROM INVENTORY
    ),
    risk_analysis AS (
        SELECT 
            organization,
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
        SELECT 
            *,
            CASE 
                WHEN days_left <= lead_time_days THEN 'HIGH'
                ELSE 'NORMAL'
            END as risk_status
        FROM risk_analysis
    )
    SELECT 
        COUNT(DISTINCT organization) as total_organizations,
        COUNT(DISTINCT item) as total_items,
        SUM(CASE WHEN risk_status = 'HIGH' THEN 1 ELSE 0 END) as high_risk_count
    FROM risk_status
    """
    
    overview = execute_query(overview_query)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="🏢 Total Organizations",
            value=int(overview['TOTAL_ORGANIZATIONS'].iloc[0])
        )
    
    with col2:
        st.metric(
            label="📦 Total Items Tracked",
            value=int(overview['TOTAL_ITEMS'].iloc[0])
        )
    
    with col3:
        st.metric(
            label="⚠️ HIGH Risk Alerts",
            value=int(overview['HIGH_RISK_COUNT'].iloc[0]),
            delta=None,
            delta_color="inverse"
        )
    
    st.markdown("---")
    
    # =========================================
    # SECTION 2: Inventory Heatmap
    # =========================================
    st.header("🗺️ Inventory Heatmap")
    st.markdown("Visual representation of closing stock levels by item and location")
    
    heatmap_query = """
    SELECT 
        item,
        location,
        SUM(closing_stock) as total_closing_stock
    FROM INVENTORY
    GROUP BY item, location
    ORDER BY item, location
    """
    
    heatmap_data = execute_query(heatmap_query)
    
    if not heatmap_data.empty:
        # Pivot data for heatmap
        pivot_data = heatmap_data.pivot(
            index='ITEM',
            columns='LOCATION',
            values='TOTAL_CLOSING_STOCK'
        ).fillna(0)
        
        # Create heatmap
        fig = px.imshow(
            pivot_data,
            labels=dict(x="Location", y="Item", color="Closing Stock"),
            x=pivot_data.columns,
            y=pivot_data.index,
            color_continuous_scale="YlOrRd",
            aspect="auto"
        )
        
        fig.update_layout(
            title="Closing Stock by Item and Location",
            xaxis_title="Location",
            yaxis_title="Item",
            height=400
        )
        
        st.plotly_chart(fig)
    else:
        st.info("No data available for heatmap. Please load sample data.")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 3: Stock-Out Alerts
    # =========================================
    st.header("⚠️ Stock-Out Alerts (HIGH Risk)")
    st.markdown("Items at high risk of stock-out (days left ≤ lead time)")
    
    # Build alerts query with filters (using safe SQL escaping)
    where_clauses = []
    if selected_org != 'All':
        # Use replace to escape single quotes for SQL safety
        safe_org = selected_org.replace("'", "''")
        where_clauses.append(f"organization = '{safe_org}'")
    if selected_loc != 'All':
        safe_loc = selected_loc.replace("'", "''")
        where_clauses.append(f"location = '{safe_loc}'")
    if selected_item != 'All':
        safe_item = selected_item.replace("'", "''")
        where_clauses.append(f"item = '{safe_item}'")
    
    where_str = " AND " + " AND ".join(where_clauses) if where_clauses else ""
    
    alerts_query = f"""
    WITH analytics AS (
        SELECT 
            organization,
            location,
            item,
            closing_stock,
            lead_time_days,
            AVG(issued) OVER (PARTITION BY organization, location, item) as avg_daily_usage
        FROM INVENTORY
        WHERE 1=1 {where_str}
    ),
    risk_analysis AS (
        SELECT 
            organization,
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
        SELECT 
            *,
            CASE 
                WHEN days_left <= lead_time_days THEN 'HIGH'
                ELSE 'NORMAL'
            END as risk_status
        FROM risk_analysis
    )
    SELECT 
        organization,
        location,
        item,
        closing_stock,
        ROUND(avg_daily_usage, 2) as avg_daily_usage,
        ROUND(days_left, 2) as days_left,
        lead_time_days,
        risk_status
    FROM risk_status
    WHERE risk_status = 'HIGH'
    ORDER BY days_left ASC
    """
    
    alerts = execute_query(alerts_query)
    
    if not alerts.empty:
        # Calculate priority scores for HIGH-risk items only
        alerts['PRIORITY_SCORE'] = alerts.apply(calculate_priority_score, axis=1)
        # Sort by priority (highest first) instead of days_left
        alerts = alerts.sort_values('PRIORITY_SCORE', ascending=False)
        
        st.dataframe(
            alerts,

            hide_index=True,
            column_config={
                "ORGANIZATION": "Organization",
                "LOCATION": "Location",
                "ITEM": "Item",
                "CLOSING_STOCK": st.column_config.NumberColumn("Closing Stock", format="%d units"),
                "AVG_DAILY_USAGE": st.column_config.NumberColumn("Avg Daily Usage", format="%.2f units"),
                "DAYS_LEFT": st.column_config.NumberColumn("Days Left", format="%.2f days"),
                "LEAD_TIME_DAYS": st.column_config.NumberColumn("Lead Time", format="%d days"),
                "RISK_STATUS": "Risk Status",
                "PRIORITY_SCORE": st.column_config.NumberColumn(
                    "Priority", 
                    format="%.1f",
                    help="Higher score = more urgent. Based on lead time, usage, and criticality."
                )
            }
        )
        
        # Show explanations for top 3 alerts
        st.subheader("📝 Detailed Explanations")
        for idx, row in alerts.head(3).iterrows():
            explanation = generate_explanation(row.to_dict())
            st.warning(f"**{row['ITEM']}**: {explanation}")
    else:
        st.success("✅ No HIGH-risk alerts! All inventory levels are healthy.")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 4: Reorder Recommendations
    # =========================================
    st.header("🔄 Reorder Recommendations")
    st.markdown("Items requiring reorder with calculated quantities")
    
    reorder_query = f"""
    WITH analytics AS (
        SELECT 
            organization,
            location,
            item,
            closing_stock,
            lead_time_days,
            AVG(issued) OVER (PARTITION BY organization, location, item) as avg_daily_usage
        FROM INVENTORY
        WHERE 1=1 {where_str}
    ),
    risk_analysis AS (
        SELECT 
            organization,
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
        SELECT 
            *,
            GREATEST(0, (lead_time_days * avg_daily_usage) - closing_stock) as reorder_qty
        FROM risk_analysis
    ),
    urgency AS (
        SELECT 
            *,
            CASE 
                WHEN days_left <= 0 THEN 'CRITICAL'
                WHEN days_left <= lead_time_days * 0.5 THEN 'CRITICAL'
                WHEN days_left <= lead_time_days THEN 'HIGH'
                ELSE 'MEDIUM'
            END as urgency_level
        FROM reorder_calc
    )
    SELECT 
        organization,
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
    ORDER BY urgency_level DESC, days_left ASC
    """
    
    reorder = execute_query(reorder_query)
    
    if not reorder.empty:
        st.dataframe(
            reorder,

            hide_index=True,
            column_config={
                "ORGANIZATION": "Organization",
                "LOCATION": "Location",
                "ITEM": "Item",
                "CLOSING_STOCK": st.column_config.NumberColumn("Current Stock", format="%d units"),
                "AVG_DAILY_USAGE": st.column_config.NumberColumn("Avg Daily Usage", format="%.2f units"),
                "DAYS_LEFT": st.column_config.NumberColumn("Days Left", format="%.2f days"),
                "LEAD_TIME_DAYS": st.column_config.NumberColumn("Lead Time", format="%d days"),
                "REORDER_QTY": st.column_config.NumberColumn("Reorder Qty", format="%d units"),
                "URGENCY_LEVEL": "Urgency"
            }
        )
        
        # Summary by urgency
        st.subheader("📈 Reorder Summary by Urgency")
        urgency_summary = reorder.groupby('URGENCY_LEVEL').agg({
            'REORDER_QTY': 'sum',
            'ITEM': 'count'
        }).reset_index()
        urgency_summary.columns = ['Urgency Level', 'Total Reorder Qty', 'Number of Items']
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.dataframe(urgency_summary, use_container_width=True, hide_index=True)
        
        with col2:
            fig = px.pie(
                urgency_summary,
                values='Number of Items',
                names='Urgency Level',
                title='Items by Urgency Level',
                color='Urgency Level',
                color_discrete_map={'CRITICAL': '#d00', 'HIGH': '#f80', 'MEDIUM': '#fd0'}
            )
            st.plotly_chart(fig)
    else:
        st.success("✅ No reorder recommendations at this time.")

except Exception as e:
    st.error(f"❌ Error: {str(e)}")
    st.info("💡 Make sure to:\n1. Create a .env file based on .env.example\n2. Test Snowflake connection\n3. Initialize database\n4. Load sample data")
    
    with st.expander("Show Error Details"):
        st.code(str(e))

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
    <p><strong>IntelliStock</strong> - AI-Driven Inventory Health & Stock-Out Alert System</p>
    <p>Built for Snowflake AI for Good Hackathon | Analytics Powered by Snowflake</p>
    <p><em>Early risk detection for organizations managing essential goods</em></p>
</div>
""", unsafe_allow_html=True)
