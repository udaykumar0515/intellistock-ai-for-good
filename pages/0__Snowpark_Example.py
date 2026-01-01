import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake_connector import create_snowpark_session

st.title("Snowpark example — Quick test :rocket:")

# Try to reuse an active Snowpark session created elsewhere (e.g., in app startup)
session = get_active_session()
if session is None:
    st.info("No active Snowpark session found. Trying to initialize one from .env...")
    try:
        session = create_snowpark_session()
        st.success("Snowpark session initialized")
    except Exception as e:
        st.error(f"Could not initialize Snowpark session: {e}")
        st.stop()

# Interactive slider
hifives_val = st.slider(
    "Number of high-fives in Q3",
    min_value=0,
    max_value=90,
    value=60,
    help="Use this to enter the number of high-fives you gave in Q3",
)

# Create a small Snowpark DataFrame and convert to pandas for plotting
created_dataframe = session.create_dataframe(
    [[50, 25, "Q1"], [20, 35, "Q2"], [hifives_val, 30, "Q3"]],
    schema=["HIGH_FIVES", "FIST_BUMPS", "QUARTER"],
)
queried_data = created_dataframe.to_pandas()

st.subheader("Number of high-fives")
st.bar_chart(data=queried_data, x="QUARTER", y="HIGH_FIVES")

st.subheader("Underlying data")
st.dataframe(queried_data, use_container_width=True)
