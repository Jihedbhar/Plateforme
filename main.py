# main.py
import streamlit as st
import logging

# Set page config
st.set_page_config(
    page_title="B2C Retailer Analytics",
    page_icon="🏪",
    layout="wide"
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize clean session state
def init_session_state():
    """Initialize session state with clean defaults"""
    defaults = {
        'source_engine': None,
        'source_conn': None,
        'source_tables': [],
        'source_table_columns': {},
        'table_mapping': {},
        'column_mappings': {},
        'csv_paths': {},
        'db_type': None,
        'db_params': {},
        'uploaded_filename': None
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

# Initialize app
init_session_state()

# Main interface
st.title("🏪 B2C Retailer Analytics")
st.markdown("**Advanced ETL Pipeline for French Retail Data**")

# Simple navigation
page = st.sidebar.selectbox(
    "Navigate",
    ["Setup", "Transform & Export", "Dashboard"],
    help="Choose a step in the ETL pipeline"
)

# Page routing
if page == "Setup":
    from app_pages.setup import setup_page
    setup_page()
elif page == "Transform & Export":
    from app_pages.transform import transform_page
    transform_page()
elif page == "Dashboard":
    from app_pages.dashboard import dashboard_page
    dashboard_page()