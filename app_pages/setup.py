# pages/setup.py
import streamlit as st
import os
import json
from datetime import datetime
from utils.config import TEMP_DIR, MAPPINGS_DIR, DB_TYPES, DEFAULT_TABLE_MAPPINGS
from utils.db_utils import create_connection, get_table_info
import logging

logger = logging.getLogger(__name__)

def setup_page():
    st.header("🔌 Setup: Database Connection & Table Mapping")

    # --- Database Connection Section ---
    st.subheader("📂 Database Connection")

    # Load existing database parameters
    if not st.session_state.get('db_path') and os.path.exists(os.path.join(TEMP_DIR, "db_*.sqlite")):
        db_files = [f for f in os.listdir(TEMP_DIR) if f.startswith("db_") and f.endswith(".sqlite")]
        if db_files:
            st.session_state.db_path = os.path.join(TEMP_DIR, db_files[-1])
            st.session_state.db_type = "sqlite"
            st.session_state.db_params = {}

    if not st.session_state.get('db_params') and os.path.exists(os.path.join(MAPPINGS_DIR, "db_params.json")):
        with open(os.path.join(MAPPINGS_DIR, "db_params.json"), "r") as f:
            st.session_state.db_params = json.load(f)
            st.session_state.db_type = "postgresql" if st.session_state.db_params.get('port') == "5432" else "mysql"

    # Database selection
    db_type = st.selectbox(
        "Select Database Type",
        options=list(DB_TYPES.keys()),
        help="Choose your database type."
    )

    db_params = {}
    if db_type == "SQLite":
        # Display previously uploaded file if available
        if st.session_state.get('db_path'):
            st.info(f"Previously uploaded: {os.path.basename(st.session_state.db_path)}")
            if st.button("Clear Uploaded File"):
                st.session_state.db_path = None
                st.session_state.db_type = None
                st.session_state.db_params = {}
                st.session_state.source_conn = None
                st.session_state.source_engine = None
                st.experimental_rerun()

        uploaded_file = st.file_uploader(
            "Upload SQLite Database (.sqlite)",
            type=["sqlite", "db", "sqlite3"],
            key="db_uploader"
        )
        if uploaded_file:
            db_filename = f"db_{uploaded_file.name}_{hash(uploaded_file.name)}_{int(datetime.now().timestamp())}.sqlite"
            db_path = os.path.join(TEMP_DIR, db_filename)
            with open(db_path, "wb") as f:
                f.write(uploaded_file.read())
            db_params['db_path'] = db_path
            st.session_state.db_path = db_path
            st.session_state.db_type = db_type
            logger.info(f"Set db_type to {db_type} and db_path to {db_path}")
    else:
        with st.form("db_form"):
            db_params['host'] = st.text_input("Host", value="localhost")
            db_params['port'] = st.text_input("Port", value="5432" if db_type == "PostgreSQL" else "3306")
            db_params['database'] = st.text_input("Database Name")
            db_params['user'] = st.text_input("Username")
            db_params['password'] = st.text_input("Password", type="password")
            submitted = st.form_submit_button("🔗 Connect")
            if submitted:
                st.session_state.db_params = db_params
                st.session_state.db_type = db_type
                logger.info(f"Set db_type to {db_type} and db_params to {db_params}")

    # Connect to source database
    if (db_type == "SQLite" and db_params.get('db_path')) or (db_type != "SQLite" and submitted):
        if db_type != "SQLite" and not all([db_params.get('host'), db_params.get('port'), db_params.get('database'), db_params.get('user')]):
            st.error("Please provide all database connection details.")
        else:
            with st.spinner("Connecting to database..."):
                source_conn, source_engine = create_connection(DB_TYPES[db_type], db_params)
                if source_conn and source_engine:
                    st.session_state.source_engine = source_engine
                    st.session_state.source_conn = source_conn
                    st.session_state.db_type = db_type
                    if db_type == "SQLite":
                        st.session_state.db_params = {'db_path': db_path}
                    if db_type != "SQLite":
                        with open(os.path.join(MAPPINGS_DIR, "db_params.json"), "w") as f:
                            json.dump(db_params, f)
                    st.success(f"✅ Connected to {db_type} database!")
                    logger.info(f"Successfully connected to {db_type} database")
                else:
                    st.error("❌ Connection failed. Check credentials or file.")
                    logger.error("Database connection failed")

    # Display database status in sidebar
    st.sidebar.markdown("### 📊 Setup Status")
    if st.session_state.get('db_type') == 'sqlite' and st.session_state.get('db_path'):
        st.write(f"📂 Database: {os.path.basename(st.session_state.db_path)} (SQLite)")
    elif st.session_state.get('db_type') in ['postgresql', 'mysql'] and st.session_state.get('db_params'):
        st.write(f"📂 Database: {st.session_state.db_params.get('database', 'Unknown')} ({st.session_state.db_type})")
    else:
        st.write("⚠️ Database not fully configured")
    if st.session_state.table_mapping:
        st.write("✅ Tables mapped")
    else:
        st.write("⚠️ Table mappings not yet defined")

    # --- Table Mapping Section ---
    if st.session_state.get('source_engine'):
        st.subheader("🔗 Table Mapping")

        # Debug: List source tables
        source_tables, source_table_columns = get_table_info(st.session_state.source_engine)
        st.markdown("**Available Source Tables in Database:**")
        if source_tables:
            st.write(", ".join(source_tables))
            logger.info(f"Source tables available: {source_tables}")
            if 'Transactions' not in source_tables and 'transactions' not in source_tables:
                st.error("⚠️ No table named 'Transactions' or 'transactions' found in the source database. Please verify the database contents.")
        else:
            st.error("⚠️ No tables found in the source database. Please check the database file or connection.")
            logger.error("No tables found in source database")
            return

        # Target database connection
        target_db_path = st.text_input(
            "Target SQLite schema path (e.g., store.sqlite):",
            value="store.sqlite",
            key="target_db_path"
        )

        target_conn, target_engine = None, None
        if target_db_path:
            with st.spinner("Connecting to target schema..."):
                target_conn, target_engine = create_connection("sqlite", {'db_path': target_db_path})
                if target_conn and target_engine:
                    target_tables, target_table_columns = get_table_info(target_engine)
                    if not target_tables:
                        st.warning("⚠️ No tables in target database.")
                    else:
                        st.success("✅ Connected to target schema!")
                        st.session_state.target_engine = target_engine
                        st.session_state.target_conn = target_conn
                else:
                    st.error("❌ Invalid target database path.")
                    return

        if target_tables:
            table_mapping = {}
            col1, col2 = st.columns([1, 2])
            with col1:
                st.subheader("Target Tables")
                for table in target_tables:
                    st.write(f"📋 {table}")

            with col2:
                st.subheader("Source Tables")
                for target_table in target_tables:
                    default_mapped_table = DEFAULT_TABLE_MAPPINGS.get(target_table, 'None')
                    used_tables = [t for t in table_mapping.values() if t != 'None']
                    valid_options = ['None'] + [t for t in source_tables if t not in used_tables]
                    default_index = valid_options.index(default_mapped_table) if default_mapped_table in valid_options else 0
                    selected_table = st.selectbox(
                        f"Map **{target_table}** to:",
                        options=valid_options,
                        index=default_index,
                        key=f"table_map_{target_table}"
                    )
                    if selected_table == 'None':
                        st.warning(f"⚠️ No source table mapped for target table '{target_table}'. It will be skipped.")
                    else:
                        table_mapping[target_table] = selected_table

            if table_mapping:
                st.session_state.table_mapping = table_mapping
                st.success("✅ Table mappings saved to session state.")
                logger.info(f"Table mappings: {table_mapping}")
                # Debug: Check Transactions mapping
                if 'Transactions' in table_mapping:
                    st.info(f"Transactions mapped to source table: {table_mapping['Transactions']}")
                else:
                    st.warning("⚠️ Transactions table not mapped. Please map it to a source table.")
            else:
                st.warning("⚠️ No tables mapped yet.")

        # Clean up target connection
        if target_conn:
            try:
                target_conn.close()
            except:
                pass
            target_engine.dispose()

    # Clean up source connection
    if st.session_state.source_conn:
        try:
            st.session_state.source_conn.close()
        except:
            pass
        st.session_state.source_engine.dispose()