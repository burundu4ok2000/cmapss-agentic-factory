import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from datetime import datetime, timedelta, date
from typing import Tuple

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PROJECT_ID = "de-zoomcamp-2026-485615"
DATASET_ID = "cmapss_telemetry"

def get_bq_client() -> bigquery.Client:
    """
    Initializes and returns a BigQuery client.
    Expects environment credentials or ADC to be configured.
    """
    return bigquery.Client(project=PROJECT_ID)

@st.cache_data(ttl=60)
def load_engine_health(unit_number: int, start_date: date, end_date: date) -> pd.DataFrame:
    """
    Fetches temporal engine health metrics for a specific unit.
    """
    client = get_bq_client()
    query = f"""
        SELECT 
            time_cycles,
            T50,
            t50_moving_avg_10,
            event_timestamp
        FROM `{PROJECT_ID}.{DATASET_ID}.mart_engine_health`
        WHERE unit_number = {unit_number}
        AND DATE(event_timestamp) BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY time_cycles ASC
    """
    return client.query(query).to_dataframe()


# ==============================================================================
# UI INTERFACE
# ==============================================================================
st.set_page_config(
    layout="wide", 
    page_title="C-MAPSS Factory 4.0 Dashboard",
    page_icon="✈️"
)

st.title("✈️ C-MAPSS Factory 4.0: Telemetry Monitor")

# Sidebar Filters
st.sidebar.header("🎛️ Control Panel")
unit_id = st.sidebar.number_input("Select Unit Number (ID)", min_value=1, max_value=1000, value=1)

# Handle date range selection gracefully
today = date.today()
epoch_start = date(2026, 1, 1)
date_selection = st.sidebar.date_input("Observation Period", value=(epoch_start, today))

# Main Content Logic
if isinstance(date_selection, (list, tuple)) and len(date_selection) == 2:
    start_dt, end_dt = date_selection
    
    st.subheader(f"📈 Engine Temporal Health: Unit {unit_id}")
    try:
        health_df = load_engine_health(unit_id, start_dt, end_dt)
        
        if health_df.empty:
            st.info("📡 Ожидание потока данных для этого двигателя (или данных за период нет)...")
        else:
            fig = px.line(
                health_df, 
                x="time_cycles", 
                y=["T50", "t50_moving_avg_10"],
                title=f"Exhaust Gas Temperature (T50) Trend for Unit {unit_id}",
                labels={"value": "Temperature (K)", "time_cycles": "Cycle Count", "variable": "Metric"},
                template="plotly_dark",
                color_discrete_map={"T50": "#3498db", "t50_moving_avg_10": "#e74c3c"}
            )
            st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Error querying BigQuery: {e}")
else:
    st.info("Пожалуйста, выберите полный диапазон дат в сайдбаре.")

st.markdown("---")
st.caption("Principal Data Engineer Access | de-zoomcamp-2026 project")
