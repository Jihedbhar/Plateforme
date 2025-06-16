# main.py
import streamlit as st
import logging
import os
from utils.config import TEMP_DIR, MAPPINGS_DIR
import json
# Set page config as the first Streamlit command
st.set_page_config(
    page_title="B2C Retailer Analytics",
    page_icon="🏪",
    layout="wide"
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize session state
# In main.py, modify init_session_state
def init_session_state():
    if 'csv_paths' not in st.session_state:
        st.session_state.csv_paths = {}
    if 'table_mapping' not in st.session_state:
        st.session_state.table_mapping = {}
    if 'column_mappings' not in st.session_state:
        st.session_state.column_mappings = {}
    if 'source_engine' not in st.session_state:
        st.session_state.source_engine = None
    if 'source_conn' not in st.session_state:
        st.session_state.source_conn = None
    if 'db_path' not in st.session_state and os.path.exists(os.path.join(TEMP_DIR, "db_*.sqlite")):
        db_files = [f for f in os.listdir(TEMP_DIR) if f.startswith("db_") and f.endswith(".sqlite")]
        if db_files:
            st.session_state.db_path = os.path.join(TEMP_DIR, db_files[-1])
            st.session_state.db_type = "sqlite"
            st.session_state.db_params = {'db_path': st.session_state.db_path}
    elif 'db_params' not in st.session_state and os.path.exists(os.path.join(MAPPINGS_DIR, "db_params.json")):
        with open(os.path.join(MAPPINGS_DIR, "db_params.json"), "r") as f:
            st.session_state.db_params = json.load(f)
            st.session_state.db_type = "postgresql" if st.session_state.db_params.get('port') == "5432" else "mysql"
    if 'target_engine' not in st.session_state:
        st.session_state.target_engine = None
    if 'target_conn' not in st.session_state:
        st.session_state.target_conn = None

# Main app
init_session_state()

st.title("🏪 B2C Retailer Analytics - Advanced ETL Pipeline")
st.markdown("**Navigate through the steps to connect, map, transform, and analyze your data.**")

# Navigation
page = st.sidebar.selectbox(
    "Select Page",
    ["Setup", "Transform & Export", "Dashboard"]
)

if page == "Setup":
    from app_pages.setup import setup_page
    setup_page()
elif page == "Transform & Export":
    from app_pages.transform import transform_page
    transform_page()
elif page == "Dashboard":
    from app_pages.dashboard import dashboard_page
    dashboard_page()