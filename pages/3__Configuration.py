"""
IntelliStock v2.1 - Configuration Page
Customize criticality scoring rules
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import json

st.set_page_config(page_title="Configuration - IntelliStock", page_icon="⚙️", layout="wide")

st.title("⚙️ Configuration")
st.markdown("Customize how IntelliStock calculates priority scores for inventory alerts.")

st.markdown("---")

# Load config
def load_criticality_config():
    config_path = Path(__file__).parent.parent / 'criticality_config.json'
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except:
            pass
    return {
        "location_rules": [{"pattern": "Emergency Unit", "score": 10, "description": "Critical emergency care location"}],
        "item_rules": [
            {"items": ["Paracetamol", "Insulin", "Syringes", "Bandages", "Masks", "Gloves"], "score": 7, "description": "Critical medical supplies"},
            {"items": ["Rice"], "score": 5, "description": "Essential food supplies"}
        ],
        "default_score": 3
    }

if 'criticality_config' not in st.session_state:
    st.session_state.criticality_config = load_criticality_config()

config = st.session_state.criticality_config

# Display current config
st.header("📊 Current Criticality Rules")

st.markdown("""
Priority score = (Lead Time × 2) + (Avg Daily Usage × 1.5) + **Criticality** - (Current Stock × 0.5)

The **Criticality** value is determined by these rules:
""")

st.markdown("---")

# Location Rules
st.subheader("📍 Location Rules")
st.caption("Assign higher scores to critical locations like emergency units")

for i, rule in enumerate(config['location_rules']):
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        new_pattern = st.text_input(
            f"Location Pattern {i+1}",
            value=rule['pattern'],
            key=f"loc_pattern_{i}",
            help="Text to match in location name (case-sensitive)"
        )
        rule['pattern'] = new_pattern
    with col2:
        new_score = st.number_input(
            f"Score",
            min_value=1,
            max_value=15,
            value=rule['score'],
            key=f"loc_score_{i}",
            help="Higher = more critical"
        )
        rule['score'] = new_score
    with col3:
        new_desc = st.text_input(
            "Description",
            value=rule['description'],
            key=f"loc_desc_{i}"
        )
        rule['description'] = new_desc

st.markdown("---")

# Item Rules
st.subheader("📦 Item Rules")
st.caption("Assign higher scores to critical items like essential medicines")

for i, rule in enumerate(config['item_rules']):
    col1, col2 = st.columns([4, 1])
    with col1:
        items_str = ", ".join(rule['items'])
        st.text_area(
            f"Items in Group {i+1}",
            value=items_str,
            key=f"items_textarea_{i}",
            help="Comma-separated list of item names",
            disabled=True  # Make read-only for now
        )
        st.caption(rule['description'])
    with col2:
        new_score = st.number_input(
            f"Score",
            min_value=1,
            max_value=15,
            value=rule['score'],
            key=f"item_score_{i}",
            label_visibility="collapsed",
            help="Higher = more critical"
        )
        rule['score'] = new_score

st.markdown("---")

# Default Score
st.subheader("🔢 Default Score")
st.caption("Score for items not matching any rule")

config['default_score'] = st.number_input(
    "Default criticality score",
    min_value=1,
    max_value=10,
    value=config['default_score'],
    help="Applied to all items that don't match location or item rules"
)

st.markdown("---")

# Save and Reset buttons
col1, col2, col3 = st.columns([1, 1, 2])

with col1:
    if st.button("💾 Save Configuration", type="primary"):
        config_path = Path(__file__).parent.parent / 'criticality_config.json'
        try:
            with open(config_path, 'w') as f:
                json.dump(config, f, indent=2)
            st.session_state.criticality_config = config
            st.success("✅ Configuration saved!")
            st.info("📊 Go to Dashboard to see updated priority scores")
        except Exception as e:
            st.error(f"❌ Error saving: {str(e)}")

with col2:
    if st.button("🔄 Reset to Defaults"):
        st.session_state.criticality_config = load_criticality_config()
        st.success("✅ Reset to default configuration!")
        st.rerun()

st.markdown("---")

# Help section
with st.expander("ℹ️ How Criticality Scoring Works"):
    st.markdown("""
    ### Priority Calculation Formula
    
    ```
    Priority = (Lead Time × 2) + (Daily Usage × 1.5) + Criticality - (Stock × 0.5)
    ```
    
    **Components:**
    - **Lead Time (×2):** Longer supplier lead times increase urgency
    - **Daily Usage (×1.5):** Higher consumption rates increase urgency  
    - **Criticality:** Location/item importance (what you configure here)
    - **Current Stock (×0.5):** Lower stock increases urgency
    
    ### Criticality Rules
    
    1. **Location Rules:** Matched against the location name
       - If "Emergency Unit" is in the location → apply higher score
    
    2. **Item Rules:** Matched against the item name
       - If item is in the critical items list → apply higher score
    
    3. **Default Score:** Used when no rules match
    
    ### Examples
    
    **High Priority Item:**
    - Paracetamol at Emergency Unit → Gets location score (10) + item score (7) = criticality of 10 (max of both)
    - High daily usage + long lead time → Very high priority
    
    **Medium Priority Item:**
    - Rice at Main Warehouse → Gets item score (5) only
    - Moderate priority based on usage and stock
    
    **Low Priority Item:**
    - Office Supplies at Admin → Gets default score (3)
    - Lower priority unless stock is critically low
    """)

st.markdown("---")
st.caption("IntelliStock v2.1 | Configuration")
