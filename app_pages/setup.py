# pages/setup.py
import streamlit as st
import os
import pandas as pd
from utils.config import DB_TYPES, DEFAULT_TABLE_MAPPINGS, DEFAULT_COLUMN_MAPPINGS
from utils.db_utils import create_connection, get_table_info
import logging

logger = logging.getLogger(__name__)

def setup_page():
    st.header("🔌 Database Connection & Table Mapping")
    
    # Progress indicator
    progress_col1, progress_col2, progress_col3 = st.columns(3)
    with progress_col1:
        db_connected = bool(st.session_state.get('source_engine'))
        st.metric("Database", "Connected ✅" if db_connected else "Not Connected ❌")
    with progress_col2:
        tables_found = len(st.session_state.get('source_tables', []))
        st.metric("Tables Found", f"{tables_found}")
    with progress_col3:
        mapping_complete = bool(st.session_state.get('table_mapping'))
        st.metric("Mapping", "Complete ✅" if mapping_complete else "Pending ❌")

    # --- Step 1: Database Connection ---
    st.subheader("📂 Step 1: Connect to Your Database")
    
    # Show expected schema info
    with st.expander("ℹ️ What tables does this app expect?", expanded=False):
        st.markdown("""
        **Expected Tables for French Retail Analytics:**
        
        - **Transactions**: Customer purchase data with dates, amounts, products
        - **Client**: Customer information and demographics  
        - **Produit**: Product catalog with categories, prices
        - **Magasin**: Store information including location and type
        - **Stock**: Inventory levels per store and product
        - **Employé**: Employee information per store
        - **Localisation**: Geographic location data
        
        *Don't worry if your table names are different - you'll map them in the next step!*
        """)

    # Database type selection
    db_type = st.selectbox(
        "Select Database Type",
        options=list(DB_TYPES.keys()),
        help="Choose your database type"
    )

    # Connection interface based on type
    if db_type == "SQLite":
        handle_sqlite_connection()
    else:
        handle_external_db_connection(db_type)

    # --- Step 2: Table Discovery ---
    if st.session_state.get('source_engine'):
        discover_tables()

    # --- Step 3: Table Mapping ---
    if st.session_state.get('source_tables'):
        handle_table_mapping()

    # Sidebar status
    show_sidebar_status()

def handle_sqlite_connection():
    """Handle SQLite database connection via file upload"""
    st.markdown("**Upload your SQLite database file:**")
    
    # Show current connection status if already connected
    if st.session_state.get('source_engine') and st.session_state.get('uploaded_filename'):
        st.success(f"✅ Currently connected to: **{st.session_state.uploaded_filename}**")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Upload Different File", type="secondary"):
                # Clear current connection
                clear_connection()
                st.rerun()
        with col2:
            if st.button("❌ Disconnect", type="secondary"):
                clear_connection()
                st.rerun()
        return
    
    uploaded_file = st.file_uploader(
        "Choose SQLite file",
        type=["sqlite", "db", "sqlite3"],
        help="Upload your SQLite database file"
    )
    
    if uploaded_file is not None:
        # Process the uploaded file directly in memory
        file_bytes = uploaded_file.read()
        
        # Create a temporary file in memory for SQLAlchemy
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp_file:
            tmp_file.write(file_bytes)
            tmp_path = tmp_file.name
        
        with st.spinner("Connecting to database..."):
            try:
                source_conn, source_engine = create_connection(DB_TYPES["SQLite"], {'db_path': tmp_path})
                if source_conn and source_engine:
                    st.session_state.source_engine = source_engine
                    st.session_state.source_conn = source_conn
                    st.session_state.db_type = "SQLite"
                    st.session_state.uploaded_filename = uploaded_file.name
                    st.session_state.connection_established = True
                    st.success(f"✅ Connected to **{uploaded_file.name}** successfully!")
                    
                    # Immediately try to discover tables
                    try:
                        source_tables, source_table_columns = get_table_info(source_engine)
                        st.session_state.source_tables = source_tables
                        st.session_state.source_table_columns = source_table_columns
                        st.info(f"📊 Found {len(source_tables)} tables. Scroll down to continue...")
                    except Exception as e:
                        st.warning(f"Connected but couldn't analyze tables: {str(e)}")
                    
                    # Force page refresh to show next steps
                    st.rerun()
                else:
                    st.error("❌ Failed to connect to the database. Please check the file.")
            except Exception as e:
                st.error(f"❌ Connection error: {str(e)}")
            finally:
                # Clean up temp file
                try:
                    os.unlink(tmp_path)
                except:
                    pass

def clear_connection():
    """Clear database connection and related state"""
    connection_keys = [
        'source_engine', 'source_conn', 'db_type', 'uploaded_filename',
        'source_tables', 'source_table_columns', 'table_mapping', 
        'connection_established', 'db_params'
    ]
    for key in connection_keys:
        if key in st.session_state:
            del st.session_state[key]

def handle_external_db_connection(db_type):
    """Handle PostgreSQL/MySQL database connections"""
    
    # Show current connection status if already connected
    if (st.session_state.get('source_engine') and 
        st.session_state.get('db_type') == db_type and 
        st.session_state.get('db_params')):
        
        db_name = st.session_state.db_params.get('database', 'Unknown')
        st.success(f"✅ Currently connected to: **{db_name}** ({db_type})")
        
        if st.button("❌ Disconnect", type="secondary"):
            clear_connection()
            st.rerun()
        return
    
    st.markdown("**Enter your database connection details:**")
    
    with st.form("db_connection_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            host = st.text_input("Host", value="localhost")
            database = st.text_input("Database Name", placeholder="retail_db")
            username = st.text_input("Username")
        
        with col2:
            port = st.text_input("Port", value="5432" if db_type == "PostgreSQL" else "3306")
            password = st.text_input("Password", type="password")
        
        connect_btn = st.form_submit_button("🔗 Connect to Database", type="primary")
        
        if connect_btn:
            if not all([host, port, database, username]):
                st.error("Please fill in all required fields.")
                return
            
            db_params = {
                'host': host,
                'port': port,
                'database': database,
                'user': username,
                'password': password
            }
            
            with st.spinner("Testing connection..."):
                try:
                    source_conn, source_engine = create_connection(DB_TYPES[db_type], db_params)
                    if source_conn and source_engine:
                        st.session_state.source_engine = source_engine
                        st.session_state.source_conn = source_conn
                        st.session_state.db_type = db_type
                        st.session_state.db_params = db_params
                        st.session_state.connection_established = True
                        st.success(f"✅ Connected to {db_type} database **{database}**!")
                        
                        # Immediately try to discover tables
                        try:
                            source_tables, source_table_columns = get_table_info(source_engine)
                            st.session_state.source_tables = source_tables
                            st.session_state.source_table_columns = source_table_columns
                            st.info(f"📊 Found {len(source_tables)} tables. Scroll down to continue...")
                        except Exception as e:
                            st.warning(f"Connected but couldn't analyze tables: {str(e)}")
                        
                        st.rerun()
                    else:
                        st.error("❌ Connection failed. Please check your credentials.")
                except Exception as e:
                    st.error(f"❌ Connection error: {str(e)}")

def discover_tables():
    """Discover and display available tables"""
    st.subheader("📊 Step 2: Available Tables")
    
    # Check if tables are already discovered
    if not st.session_state.get('source_tables'):
        with st.spinner("Analyzing database structure..."):
            try:
                source_tables, source_table_columns = get_table_info(st.session_state.source_engine)
                st.session_state.source_tables = source_tables
                st.session_state.source_table_columns = source_table_columns
            except Exception as e:
                st.error(f"Error analyzing database: {str(e)}")
                return
    
    source_tables = st.session_state.get('source_tables', [])
    source_table_columns = st.session_state.get('source_table_columns', {})
    
    if source_tables:
        st.success(f"✅ Found {len(source_tables)} tables!")
        
        # Display tables in a nice grid
        cols = st.columns(min(3, len(source_tables)))
        for i, table in enumerate(source_tables):
            with cols[i % 3]:
                column_count = len(source_table_columns.get(table, []))
                st.info(f"**{table}**\n{column_count} columns")
        
        # Table inspector
        with st.expander("🔍 Inspect Table Structure"):
            selected_table = st.selectbox("Select table:", source_tables)
            if selected_table:
                columns = source_table_columns.get(selected_table, [])
                st.write("**Columns:**")
                for col in columns:
                    st.write(f"• {col}")
                
                # Data preview
                if st.button(f"Preview {selected_table} Data"):
                    try:
                        query = f"SELECT * FROM {selected_table} LIMIT 5"
                        df = pd.read_sql(query, st.session_state.source_engine)
                        st.dataframe(df, use_container_width=True)
                    except Exception as e:
                        st.error(f"Preview error: {str(e)}")
    else:
        st.warning("⚠️ No tables found in the database.")

def handle_table_mapping():
    """Handle table mapping interface"""
    st.subheader("🔗 Step 3: Map Your Tables")
    
    st.markdown("""
    **Map your database tables to the expected schema.** The app expects specific table names for analysis.
    Select which of your tables corresponds to each expected table.
    """)
    
    # Generate expected schema from config
    expected_tables = list(DEFAULT_COLUMN_MAPPINGS.keys())
    source_tables = st.session_state.get('source_tables', [])
    
    # Show expected schema
    with st.expander("📋 Expected Schema Overview", expanded=False):
        for table_name, columns in DEFAULT_COLUMN_MAPPINGS.items():
            st.write(f"**{table_name}**: {', '.join(list(columns.keys())[:4])}{'...' if len(columns) > 4 else ''}")
    
    # Mapping interface
    table_mapping = {}
    
    st.markdown("### Configure Mappings")
    for expected_table in expected_tables:
        col1, col2, col3 = st.columns([2, 1, 2])
        
        with col1:
            st.write(f"**{expected_table}**")
            expected_columns = list(DEFAULT_COLUMN_MAPPINGS[expected_table].keys())
            st.caption(f"Expected: {', '.join(expected_columns[:3])}{'...' if len(expected_columns) > 3 else ''}")
        
        with col2:
            st.write("**→**")
        
        with col3:
            # Get default suggestion
            default_table = DEFAULT_TABLE_MAPPINGS.get(expected_table, 'None')
            
            # Available options (prevent duplicates)
            used_tables = [t for t in table_mapping.values() if t != 'None']
            available_options = ['None'] + [t for t in source_tables if t not in used_tables]
            
            # Select mapping
            if default_table in available_options:
                default_index = available_options.index(default_table)
            else:
                default_index = 0
            
            selected = st.selectbox(
                "Your table:",
                options=available_options,
                index=default_index,
                key=f"mapping_{expected_table}"
            )
            
            if selected != 'None':
                table_mapping[expected_table] = selected
                source_cols = len(st.session_state.source_table_columns.get(selected, []))
                st.success(f"✅ {source_cols} columns")
            else:
                st.warning("⚠️ Skipped")
    
    # Save mappings
    if table_mapping:
        st.markdown("---")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.metric("Tables Mapped", f"{len(table_mapping)}/{len(expected_tables)}")
        
        with col2:
            if st.button("💾 Save Mapping", type="primary"):
                st.session_state.table_mapping = table_mapping
                st.success("✅ Table mapping saved!")
                
                # Show summary
                with st.expander("📊 Mapping Summary"):
                    for target, source in table_mapping.items():
                        st.write(f"**{target}** ← {source}")
                
                st.balloons()
    else:
        st.info("ℹ️ No tables mapped yet. Map at least one table to proceed.")

def show_sidebar_status():
    """Show current status in sidebar"""
    st.sidebar.markdown("### 📊 Current Status")
    
    # Database
    if st.session_state.get('source_engine'):
        if st.session_state.get('db_type') == 'SQLite':
            filename = st.session_state.get('uploaded_filename', 'SQLite DB')
            st.sidebar.success(f"✅ Database: {filename}")
        else:
            db_name = st.session_state.get('db_params', {}).get('database', 'Connected')
            st.sidebar.success(f"✅ Database: {db_name}")
    else:
        st.sidebar.error("❌ No database connected")
    
    # Tables
    table_count = len(st.session_state.get('source_tables', []))
    if table_count > 0:
        st.sidebar.success(f"✅ {table_count} tables found")
    else:
        st.sidebar.error("❌ No tables discovered")
    
    # Mapping
    mapping_count = len(st.session_state.get('table_mapping', {}))
    if mapping_count > 0:
        st.sidebar.success(f"✅ {mapping_count} tables mapped")
    else:
        st.sidebar.error("❌ No mappings configured")
    
    # Next steps
    st.sidebar.markdown("---")
    if not st.session_state.get('source_engine'):
        st.sidebar.info("👆 Connect to your database above")
    elif not table_count:
        st.sidebar.info("🔍 Check your database connection")
    elif not mapping_count:
        st.sidebar.info("🔗 Configure table mappings")
    else:
        st.sidebar.success("🚀 Ready for next step!")
        st.sidebar.info("Navigate to **Transform & Export**")