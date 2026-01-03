"""
IntelliStock v2.1 - Dashboard Page
Decision-making dashboard with analytics, alerts, and calculators
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
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
import json

# Import Snowflake tasks helper for Dynamic Tables and Unistore
try:
    from snowflake_tasks_helper import (
        get_dynamic_table_refresh_status,
        log_action_to_unistore,
        create_order_in_unistore,
        get_system_health_dashboard
    )
    TASKS_HELPER_AVAILABLE = True
except ImportError:
    TASKS_HELPER_AVAILABLE = False

# Feature flags
USE_DYNAMIC_TABLES = True  # Set to True to use Dynamic Tables instead of CTEs
USE_UNISTORE_LOGGING = True  # Set to True to enable action logging to Unistore

# Load criticality configuration
def load_criticality_config():
    """Load criticality configuration from JSON file."""
    config_path = Path(__file__).parent.parent / 'criticality_config.json'
    
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except:
            pass
    
    # Default config if file doesn't exist or can't be loaded
    return {
        "location_rules": [{"pattern": "Emergency Unit", "score": 10, "description": "Critical emergency care location"}],
        "item_rules": [
            {"items": ["Paracetamol", "Insulin", "Syringes", "Bandages", "Masks", "Gloves"], "score": 7, "description": "Critical medical supplies"},
            {"items": ["Rice"], "score": 5, "description": "Essential food supplies"}
        ],
        "default_score": 3
    }

# Load config at startup
criticality_config = load_criticality_config()

# Page configuration
st.set_page_config(
    page_title="Dashboard - IntelliStock",
    page_icon="📊",
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
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
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
st.markdown('<div class="main-header">📊 Dashboard</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Real-time inventory analytics and decision support</div>', unsafe_allow_html=True)

# Initialize session state for tracking ordered items
if 'ordered_items' not in st.session_state:
    st.session_state.ordered_items = set()  # Store as (org, location, item) tuples

# Helper function for priority scoring
def calculate_priority_score(row, config=None):
    """Calculate action priority score for high-risk items using configurable criticality."""
    if config is None:
        config = criticality_config
    
    # Convert numeric types safely
    lead_time = float(row['LEAD_TIME_DAYS'])
    avg_daily_usage = float(row['AVG_DAILY_USAGE'])
    closing_stock = float(row['CLOSING_STOCK'])
    
    # Criticality score from config
    location = str(row.get('LOCATION', ''))
    item = str(row.get('ITEM', ''))
    
    criticality = config['default_score']
    
    # Check location rules
    for rule in config['location_rules']:
        if rule['pattern'] in location:
            criticality = max(criticality, rule['score'])
    
    # Check item rules
    for rule in config['item_rules']:
        if item in rule['items']:
            criticality = max(criticality, rule['score'])
    
    # Priority formula (unchanged)
    score = (
        (lead_time * 2.0) +
        (avg_daily_usage * 1.5) +
        criticality -
        (closing_stock * 0.5)
    )
    
    return round(score, 2)

# Helper functions for ordered item tracking
def is_ordered(org, location, item):
    """Check if item is marked as ordered."""
    return (org, location, item) in st.session_state.ordered_items

def toggle_ordered(org, location, item):
    """Toggle ordered status for an item."""
    key = (org, location, item)
    if key in st.session_state.ordered_items:
        st.session_state.ordered_items.remove(key)
    else:
        st.session_state.ordered_items.add(key)

def create_sparkline(org, location, item):
    """Generate sparkline for last 7 days of closing stock."""
    query = f"""
    WITH recent_data AS (
        SELECT date, closing_stock
        FROM INVENTORY
        WHERE organization = '{org.replace("'", "''")}'
          AND location = '{location.replace("'", "''")}'
          AND item = '{item.replace("'", "''")}'
        ORDER BY date DESC
        LIMIT 7
    )
    SELECT date, closing_stock
    FROM recent_data
    ORDER BY date ASC
    """
    
    try:
        df = execute_query(query)
        if df.empty or len(df) < 2:
            return None
        
        # Create minimal Plotly sparkline
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['DATE'],
            y=df['CLOSING_STOCK'],
            mode='lines',
            line=dict(width=2, color='#1f77b4'),
            fill='tozeroy',
            fillcolor='rgba(31, 119, 180, 0.2)'
        ))
        
        fig.update_layout(
            showlegend=False,
            height=50,
            margin=dict(l=0, r=0, t=0, b=0),
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            hovermode=False
        )
        
        return fig
    except Exception as e:
        return None

def generate_action_panel_pdf(top_actions_df):
    """Generate PDF of Today's Action Panel."""
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.units import inch
    from io import BytesIO
    from datetime import datetime
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    elements = []
    styles = getSampleStyleSheet()
    
    # Title
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#1f77b4'),
        spaceAfter=30,
        alignment=1  # Center
    )
    elements.append(Paragraph("IntelliStock - Today's Action Panel", title_style))
    
    # Date
    date_style = styles['Normal']
    elements.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", date_style))
    elements.append(Spacer(1, 0.3*inch))
    
    # Action items table
    table_data = [['Priority', 'Item', 'Organization', 'Location', 'Days Left', 'Score']]
    
    for idx, (_, row) in enumerate(top_actions_df.iterrows(), 1):
        table_data.append([
            str(idx),
            row['ITEM'],
            row['ORGANIZATION'],
            row['LOCATION'],
            f"{row['DAYS_LEFT']:.1f}",
            f"{row['PRIORITY_SCORE']:.1f}"
        ])
    
    table = Table(table_data, colWidths=[0.6*inch, 1.5*inch, 1.5*inch, 1.5*inch, 1*inch, 0.8*inch])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1f77b4')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
    ]))
    
    elements.append(table)
    elements.append(Spacer(1, 0.5*inch))
    
    # Signature line
    elements.append(Paragraph("Authorized by: _______________________", styles['Normal']))
    elements.append(Spacer(1, 0.2*inch))
    elements.append(Paragraph("Date: _______________________", styles['Normal']))
    
    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()

# Sidebar - Filters only (Setup and Config moved to other pages)
with st.sidebar:
    st.header("🔍 Filters")
    st.caption("Filter dashboard data by organization, location, or item")
    
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
        error_msg = str(e).lower()
        if 'does not exist' in error_msg or 'not found' in error_msg:
            st.warning("⚠️ No data found. Go to Data Management page to upload data.")
        else:
            st.warning(f"⚠️ Could not load filter options. Please check your connection.")
        org_list = ['All']
        loc_list = ['All']
        item_list = ['All']
    
    selected_org = st.selectbox("Organization", org_list)
    selected_loc = st.selectbox("Location", loc_list)
    selected_item = st.selectbox("Item", item_list, key="item_filter")
    
    
    st.markdown("---")
    
    # Data Freshness Indicator
    if USE_DYNAMIC_TABLES and TASKS_HELPER_AVAILABLE:
        st.subheader("📊 Data Freshness")
        try:
            dt_status = get_dynamic_table_refresh_status('STOCK_ANALYTICS_DT')
            if "last_refresh_end" in dt_status and dt_status['last_refresh_end'] != "N/A":
                last_refresh = dt_status['last_refresh_end']
                st.success(f"✅ Last updated: {last_refresh}")
            else:
                st.info("⏳ Refreshing...")
        except:
            st.caption("Data freshness info unavailable")
    
    st.markdown("---")
    st.info("💡 **Tip:** Visit Data Management to upload data, or Configuration to adjust scoring rules.")


# Main content
try:
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
            
            # Filter out ordered items
            top_actions['IS_ORDERED'] = top_actions.apply(
                lambda row: is_ordered(row['ORGANIZATION'], row['LOCATION'], row['ITEM']), 
                axis=1
            )
            top_actions_filtered = top_actions[~top_actions['IS_ORDERED']].copy()
            top_actions_filtered = top_actions_filtered.sort_values('PRIORITY_SCORE', ascending=False).head(3)
            
            # Display each action with clear explanation
            if not top_actions_filtered.empty:
                for idx, row in top_actions_filtered.iterrows():
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.markdown(f"**{row['ITEM']}** • {row['ORGANIZATION']} – {row['LOCATION']}")
                        # Rule-based explanation (deterministic, no AI)
                        explanation = f"Reorder {row['ITEM']} at {row['ORGANIZATION']} – {row['LOCATION']}. High daily usage and long supplier lead time make this the most urgent action."
                        st.caption(explanation)
                    with col2:
                        st.metric("Priority", f"{row['PRIORITY_SCORE']:.1f}")
                
                # Add PDF export button
                st.markdown("---")
                if st.button("📄 Export to PDF", key="export_pdf_button"):
                    from datetime import datetime
                    pdf_bytes = generate_action_panel_pdf(top_actions_filtered)
                    st.download_button(
                        label="⬇️ Download Action Panel PDF",
                        data=pdf_bytes,
                        file_name=f"intellistock_actions_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
                        mime="application/pdf",
                        key="download_pdf_button"
                    )
            else:
                st.info("✅ All urgent items have been marked as ordered!")
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
    # SECTION 2.5: What-If Order Calculator
    # =========================================
    st.header("🧮 What-If Order Calculator")
    st.markdown("Calculate how long an order will last based on current usage patterns")
    
    # Fetch high-risk items for calculator
    try:
        calculator_query = f"""
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
        )
        SELECT 
            organization,
            location,
            item,
            closing_stock,
            ROUND(avg_daily_usage, 2) as avg_daily_usage,
            ROUND(days_left, 2) as days_left,
            lead_time_days
        FROM risk_analysis
        WHERE days_left <= lead_time_days
        ORDER BY days_left ASC
        """
        
        calculator_items = execute_query(calculator_query)
        
        if not calculator_items.empty and len(calculator_items) > 0:
            # Item selector
            item_options = [
                f"{row['ITEM']} ({row['ORGANIZATION']} - {row['LOCATION']})" 
                for _, row in calculator_items.iterrows()
            ]
            
            selected_calc_idx = st.selectbox(
                "Select item to analyze",
                options=range(len(item_options)),
                format_func=lambda x: item_options[x],
                key="calc_item_select"
            )
            
            if selected_calc_idx is not None:
                selected_row = calculator_items.iloc[selected_calc_idx]
                org = selected_row['ORGANIZATION']
                loc = selected_row['LOCATION']
                item_name = selected_row['ITEM']
                current_stock = float(selected_row['CLOSING_STOCK'])
                avg_usage = float(selected_row['AVG_DAILY_USAGE'])
                lead_time = float(selected_row['LEAD_TIME_DAYS'])
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("📊 Current Situation")
                    st.metric("Current Stock", f"{current_stock:.0f} units")
                    st.metric("Avg Daily Usage", f"{avg_usage:.2f} units/day")
                    
                    # Current days left
                    current_days = current_stock / avg_usage if avg_usage > 0 else 9999
                    st.metric("Current Days Left", f"{current_days:.1f} days")
                    st.metric("Lead Time", f"{lead_time:.0f} days")
                
                with col2:
                    st.subheader("🔮 Order Projection")
                    
                    # Default: enough for 30 days
                    default_order = max(10, int(avg_usage * 30) - int(current_stock))
                    
                    order_qty = st.number_input(
                        "Order Quantity (units)",
                        min_value=0,
                        max_value=10000,
                        value=default_order,
                        step=10,
                        key="order_qty_input"
                    )
                    
                    # Calculations
                    new_stock = current_stock + order_qty
                    new_days = new_stock / avg_usage if avg_usage > 0 else 9999
                    days_gained = new_days - current_days
                    
                    st.metric(
                        "Projected Stock After Order",
                        f"{new_stock:.0f} units",
                        delta=f"+{order_qty} units"
                    )
                    
                    st.metric(
                        "Projected Days Left",
                        f"{new_days:.1f} days",
                        delta=f"+{days_gained:.1f} days"
                    )
                    
                    # Risk assessment
                    if new_days > lead_time * 2:
                        risk_color = "🟢"
                        risk_text = "SAFE - Well stocked"
                    elif new_days > lead_time:
                        risk_color = "🟡"
                        risk_text = "MODERATE - Adequate coverage"
                    else:
                        risk_color = "🔴"
                        risk_text = "HIGH - Still at risk"
                    
                    st.info(f"{risk_color} **Risk Level:** {risk_text}")
                
                # Suggested safe quantity
                st.markdown("---")
                st.subheader("💡 Recommendations")
                
                # Calculate for 60 days coverage
                safe_qty_60 = max(0, (60 * avg_usage) - current_stock)
                # Calculate for 90 days coverage
                safe_qty_90 = max(0, (90 * avg_usage) - current_stock)
                
                rec_col1, rec_col2 = st.columns(2)
                with rec_col1:
                    st.success(f"**60 days coverage:** Order {safe_qty_60:.0f} units")
                with rec_col2:
                    st.success(f"**90 days coverage:** Order {safe_qty_90:.0f} units")
                
        else:
            st.info("✅ No high-risk items to analyze. System is healthy!")
            
    except Exception as e:
        st.info("💡 Initialize database and load data to use the calculator")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 3: Stock-Out Alerts
    # =========================================
    st.header("⚠️ Stock-Out Alerts (HIGH Risk)")
    st.markdown("Items at high risk of stock-out (days left ≤ lead time)")
    
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
        
        # Add ordered status column
        alerts['ORDERED'] = alerts.apply(
            lambda row: is_ordered(row['ORGANIZATION'], row['LOCATION'], row['ITEM']),
            axis=1
        )
        
        # Display with ordered checkboxes
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
                ),
                "ORDERED": st.column_config.CheckboxColumn(
                    "Ordered",
                    help="Mark item as ordered",
                    default=False
                )
            }
        )
        
        # Add mark/unmark buttons
        st.markdown("##### Quick Actions")
        for button_idx, (idx, row) in enumerate(alerts.head(5).iterrows()):  # Show buttons for top 5
            item_key = f"{button_idx}_{row['ORGANIZATION']}_{row['LOCATION']}_{row['ITEM']}".replace(" ", "_")
            cols = st.columns([3, 1, 1])
            with cols[0]:
                st.text(f"{row['ITEM']} at {row['ORGANIZATION']} - {row['LOCATION']}")
            with cols[1]:
                if not is_ordered(row['ORGANIZATION'], row['LOCATION'], row['ITEM']):
                    if st.button(f"✓ Mark Ordered", key=f"mark_{item_key}"):
                        toggle_ordered(row['ORGANIZATION'], row['LOCATION'], row['ITEM'])
                        st.rerun()
            with cols[2]:
                if is_ordered(row['ORGANIZATION'], row['LOCATION'], row['ITEM']):
                    if st.button(f"✗ Unmark", key=f"unmark_{item_key}"):
                        toggle_ordered(row['ORGANIZATION'], row['LOCATION'], row['ITEM'])
                        st.rerun()
        
        # Show explanations for top 3 alerts
        st.subheader("📝 Detailed Explanations")
        for idx, row in alerts.head(3).iterrows():
            explanation = generate_explanation(row.to_dict())
            st.warning(f"**{row['ITEM']}**: {explanation}")
        
        # Add 7-day trend sparklines section
        st.markdown("---")
        st.subheader("📈 7-Day Stock Trends")
        st.caption("Visual trends of closing stock for top alerts")
        
        for idx, row in alerts.head(5).iterrows():  # Show trends for top 5
            cols = st.columns([3, 1])
            
            with cols[0]:
                st.markdown(f"**{row['ITEM']}** • {row['ORGANIZATION']} – {row['LOCATION']}")
                subcols = st.columns(4)
                with subcols[0]:
                    st.metric("Current Stock", f"{row['CLOSING_STOCK']:.0f}")
                with subcols[1]:
                    st.metric("Days Left", f"{row['DAYS_LEFT']:.1f}")
                with subcols[2]:
                    st.metric("Priority", f"{row['PRIORITY_SCORE']:.1f}")
                with subcols[3]:
                    st.metric("Avg Usage", f"{row['AVG_DAILY_USAGE']:.1f}/day")
            
            with cols[1]:
                # Generate and display sparkline
                sparkline = create_sparkline(
                    row['ORGANIZATION'],
                    row['LOCATION'],
                    row['ITEM']
                )
                if sparkline:
                    st.plotly_chart(sparkline, use_container_width=True, key=f"spark_{idx}")
                else:
                    st.caption("No trend data")
            
            st.markdown("---")
    else:
        st.success("✅ No HIGH-risk alerts! All inventory levels are healthy.")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 3.5: Ordered Today
    # =========================================
    if st.session_state.ordered_items:
        st.header("📦 Ordered Today")
        st.markdown("Items marked as ordered during this session")
        
        # Create DataFrame of ordered items by filtering alerts
        if not alerts.empty:
            ordered_items_df = alerts[alerts['ORDERED'] == True].copy()
            
            if not ordered_items_df.empty:
                # Display ordered items
                st.dataframe(
                    ordered_items_df[['ORGANIZATION', 'LOCATION', 'ITEM', 'CLOSING_STOCK', 'AVG_DAILY_USAGE', 'PRIORITY_SCORE']],
                    hide_index=True,
                    column_config={
                        "ORGANIZATION": "Organization",
                        "LOCATION": "Location",
                        "ITEM": "Item",
                        "CLOSING_STOCK": st.column_config.NumberColumn("Current Stock", format="%d units"),
                        "AVG_DAILY_USAGE": st.column_config.NumberColumn("Daily Usage", format="%.2f units"),
                        "PRIORITY_SCORE": st.column_config.NumberColumn("Priority", format="%.1f")
                    }
                )
                
                st.caption(f"Total items ordered: {len(ordered_items_df)}")
            else:
                st.info("Items marked as ordered will appear here.")
        
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
