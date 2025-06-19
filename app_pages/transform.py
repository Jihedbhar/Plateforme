# pages/transform.py
import streamlit as st
import pandas as pd
import os
import json
import tempfile
from utils.db_utils import get_table_info, escape_table_name
from utils.etl_utils import transform_and_export
from utils.config import DEFAULT_COLUMN_MAPPINGS
import logging

logger = logging.getLogger(__name__)

def transform_page():
    st.header("📊 Column Mapping & Data Transformation")
    
    # Check prerequisites
    if not st.session_state.get('source_engine'):
        st.error("⚠️ Please connect to a database first on the Setup page.")
        return
    
    if not st.session_state.get('table_mapping'):
        st.error("⚠️ Please complete table mapping first on the Setup page.")
        return

    # Progress indicator
    progress_col1, progress_col2, progress_col3 = st.columns(3)
    with progress_col1:
        mappings_done = len(st.session_state.get('column_mappings', {}))
        total_tables = len(st.session_state.get('table_mapping', {}))
        st.metric("Column Mappings", f"{mappings_done}/{total_tables}")
    with progress_col2:
        exports_done = len(st.session_state.get('csv_paths', {}))
        st.metric("Data Exports", f"{exports_done}")
    with progress_col3:
        all_complete = mappings_done == total_tables and exports_done > 0
        st.metric("Status", "Complete ✅" if all_complete else "In Progress 🔄")

    st.markdown("---")
    
    # Main transformation interface
    handle_column_mapping_and_export()
    
    # Bulk export button - positioned prominently
    show_bulk_export_section()
    
    # Final export summary
    if st.session_state.get('csv_paths'):
        show_export_summary()

def show_bulk_export_section():
    """Show bulk export section for all mapped tables"""
    
    column_mappings = st.session_state.get('column_mappings', {})
    table_mapping = st.session_state.get('table_mapping', {})
    csv_paths = st.session_state.get('csv_paths', {})
    
    # Count tables with mappings
    tables_with_mappings = []
    for target_table, source_table in table_mapping.items():
        if source_table != 'None' and target_table in column_mappings and column_mappings[target_table]:
            tables_with_mappings.append((target_table, source_table))
    
    if not tables_with_mappings:
        return
    
    st.markdown("---")
    st.header("🚀 Bulk Export")
    
    # Show what will be exported
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.info(f"**{len(tables_with_mappings)} tables** ready for export with column mappings configured")
        
        # Show summary of what will be exported
        with st.expander("📋 Export Preview", expanded=False):
            for target_table, source_table in tables_with_mappings:
                mapping = column_mappings.get(target_table, {})
                cols_mapped = len(mapping)
                st.write(f"• **{target_table}** ← {source_table} ({cols_mapped} columns)")
    
    with col2:
        # Check if any tables are already exported
        already_exported = [table for table, _ in tables_with_mappings if table in csv_paths]
        not_exported = [table for table, _ in tables_with_mappings if table not in csv_paths]
        
        if already_exported:
            st.success(f"✅ {len(already_exported)} already exported")
        if not_exported:
            st.warning(f"⏳ {len(not_exported)} pending export")
    
    # Bulk export button
    if not_exported:
        if st.button("🚀 **Export All Mapped Tables**", type="primary", use_container_width=True):
            perform_bulk_export(tables_with_mappings)
    
    # Re-export all button
    if already_exported:
        if st.button("🔄 **Re-export All Tables**", type="secondary", use_container_width=True):
            perform_bulk_export(tables_with_mappings, force_reexport=True)

def perform_bulk_export(tables_with_mappings, force_reexport=False):
    """Perform bulk export of all mapped tables"""
    
    column_mappings = st.session_state.get('column_mappings', {})
    csv_paths = st.session_state.get('csv_paths', {})
    
    # Initialize session state if needed
    if 'csv_paths' not in st.session_state:
        st.session_state.csv_paths = {}
    
    # Progress tracking
    total_tables = len(tables_with_mappings)
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    successful_exports = 0
    failed_exports = []
    
    for i, (target_table, source_table) in enumerate(tables_with_mappings):
        # Skip if already exported (unless force re-export)
        if not force_reexport and target_table in csv_paths:
            successful_exports += 1
            progress_bar.progress((i + 1) / total_tables)
            status_text.text(f"Skipping {target_table} (already exported)...")
            continue
        
        status_text.text(f"Exporting {target_table}... ({i + 1}/{total_tables})")
        
        try:
            # Get column mapping and transformations for this table
            column_mapping = column_mappings.get(target_table, {})
            transformations = []  # You can extend this to get transformations from session state
            
            # Create temporary file for export
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
            csv_path = temp_file.name
            temp_file.close()
            
            # Perform export
            success = transform_and_export(
                engine=st.session_state.source_engine,
                table_name=source_table,
                columns=list(column_mapping.values()),
                target_columns=list(column_mapping.keys()),
                output_path=csv_path,
                transformations=transformations
            )
            
            if success:
                st.session_state.csv_paths[target_table] = csv_path
                successful_exports += 1
                status_text.text(f"✅ {target_table} exported successfully!")
            else:
                failed_exports.append(target_table)
                status_text.text(f"❌ Failed to export {target_table}")
                
        except Exception as e:
            failed_exports.append(target_table)
            status_text.text(f"❌ Error exporting {target_table}: {str(e)}")
            logger.error(f"Bulk export error for {target_table}: {e}")
        
        # Update progress
        progress_bar.progress((i + 1) / total_tables)
    
    # Final status
    progress_bar.progress(1.0)
    
    if successful_exports == total_tables:
        status_text.empty()
        st.success(f"🎉 **All {total_tables} tables exported successfully!**")
        st.balloons()
    elif successful_exports > 0:
        status_text.empty()
        st.warning(f"⚠️ **{successful_exports}/{total_tables} tables exported successfully**")
        if failed_exports:
            st.error(f"Failed exports: {', '.join(failed_exports)}")
    else:
        status_text.empty()
        st.error("❌ **No tables were exported successfully**")
        if failed_exports:
            st.error(f"Failed exports: {', '.join(failed_exports)}")
    
    # Auto-refresh to show updated state
    st.rerun()

def handle_column_mapping_and_export():
    """Handle column mapping and data export for each table"""
    
    table_mapping = st.session_state.get('table_mapping', {})
    source_table_columns = st.session_state.get('source_table_columns', {})
    
    column_mappings = st.session_state.get('column_mappings', {})
    csv_paths = st.session_state.get('csv_paths', {})
    transformations = {}

    for target_table, source_table in table_mapping.items():
        if source_table == 'None':
            continue
            
        with st.expander(f"🔧 Configure {target_table} ← {source_table}", expanded=False):
            
            # Get column information
            expected_columns = list(DEFAULT_COLUMN_MAPPINGS.get(target_table, {}).keys())
            source_columns = source_table_columns.get(source_table, [])
            
            # Show column info
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Expected columns ({len(expected_columns)}):**")
                st.caption(", ".join(expected_columns[:4]) + ("..." if len(expected_columns) > 4 else ""))
            with col2:
                st.write(f"**Available columns ({len(source_columns)}):**")
                st.caption(", ".join(source_columns[:4]) + ("..." if len(source_columns) > 4 else ""))

            # Column mapping interface
            st.subheader("📋 Column Mapping")
            column_mapping = configure_column_mapping(target_table, expected_columns, source_columns)
            
            # Transformations interface
            if column_mapping:
                st.subheader("🔄 Data Transformations")
                table_transformations = configure_transformations(target_table, column_mapping, source_columns)
                if table_transformations:
                    transformations[target_table] = table_transformations

            # Show export status (but remove individual export buttons)
            if column_mapping:
                show_table_export_status(target_table, source_table)
            
            # Data preview - Using checkbox instead of nested expander
            st.subheader("👀 Data Preview")
            if st.checkbox(f"Show {source_table} data preview", key=f"preview_check_{source_table}"):
                show_data_preview(source_table)

    # Save final state
    if column_mappings != st.session_state.get('column_mappings', {}):
        st.session_state.column_mappings = column_mappings

def show_table_export_status(target_table, source_table):
    """Show export status for a table without individual export buttons"""
    
    csv_paths = st.session_state.get('csv_paths', {})
    column_mappings = st.session_state.get('column_mappings', {})
    
    st.subheader("📤 Export Status")
    
    if target_table in csv_paths:
        st.success(f"✅ **{target_table}** is ready for export (mapped columns configured)")
        
        # Show preview if already exported
        try:
            df_preview = pd.read_csv(csv_paths[target_table], nrows=3)
            st.write("**Preview (first 3 rows):**")
            st.dataframe(df_preview, use_container_width=True)
        except Exception as e:
            st.caption("Preview unavailable")
            
        # Download individual file option
        col1, col2 = st.columns(2)
        with col1:
            try:
                with open(csv_paths[target_table], 'rb') as f:
                    st.download_button(
                        label=f"📁 Download {target_table}.csv",
                        data=f.read(),
                        file_name=f"{target_table}.csv",
                        mime="text/csv",
                        key=f"individual_download_{target_table}"
                    )
            except Exception as e:
                st.error(f"Download error: {e}")
    else:
        column_mapping = column_mappings.get(target_table, {})
        if column_mapping:
            st.info(f"📋 **{target_table}** configured with {len(column_mapping)} mapped columns - ready for bulk export")
        else:
            st.warning("⚠️ Configure column mappings first")

def configure_column_mapping(target_table, expected_columns, source_columns):
    """Configure column mappings for a table"""
    
    column_mapping = {}
    default_mappings = DEFAULT_COLUMN_MAPPINGS.get(target_table, {})
    
    # Create mapping interface in columns
    num_cols = min(3, len(expected_columns))
    mapping_cols = st.columns(num_cols)
    
    for i, target_col in enumerate(expected_columns):
        with mapping_cols[i % num_cols]:
            # Get default mapping suggestion
            default_source_col = default_mappings.get(target_col, 'None')
            
            # Available options
            options = ['None'] + source_columns
            default_index = options.index(default_source_col) if default_source_col in options else 0
            
            selected_col = st.selectbox(
                f"**{target_col}**",
                options=options,
                index=default_index,
                key=f"col_map_{target_table}_{target_col}",
                help=f"Map {target_col} to a source column"
            )
            
            if selected_col != 'None':
                column_mapping[target_col] = selected_col
    
    # Show mapping summary
    if column_mapping:
        mapped_count = len(column_mapping)
        total_count = len(expected_columns)
        
        if mapped_count == total_count:
            st.success(f"✅ All {total_count} columns mapped!")
        else:
            st.info(f"📊 {mapped_count}/{total_count} columns mapped")
            unmapped = [col for col in expected_columns if col not in column_mapping]
            st.caption(f"Unmapped: {', '.join(unmapped)}")
    else:
        st.warning("⚠️ No columns mapped yet")
    
    # Save to session state
    if 'column_mappings' not in st.session_state:
        st.session_state.column_mappings = {}
    st.session_state.column_mappings[target_table] = column_mapping
    
    return column_mapping

def configure_transformations(target_table, column_mapping, source_columns):
    """Configure data transformations"""
    
    transformations = []
    
    # Only show transformation options if there are mapped columns
    if not column_mapping:
        return transformations
    
    with st.form(f"transformations_{target_table}"):
        st.markdown("**Configure data transformations (optional):**")
        
        for target_col, source_col in column_mapping.items():
            if source_col not in source_columns:
                continue
                
            col1, col2, col3 = st.columns([2, 2, 3])
            
            with col1:
                st.write(f"**{target_col}**")
                st.caption(f"From: {source_col}")
            
            with col2:
                transform_type = st.selectbox(
                    "Transform",
                    options=["None", "Fill Missing", "Cast Type"],
                    key=f"transform_{target_table}_{target_col}"
                )
            
            with col3:
                if transform_type == "Fill Missing":
                    fill_value = st.text_input(
                        "Fill value",
                        key=f"fill_{target_table}_{target_col}",
                        placeholder="e.g., 0, Unknown, N/A"
                    )
                    if fill_value:
                        transformations.append({
                            'type': 'fill_na',
                            'column': source_col,
                            'value': fill_value,
                            'target_column': target_col
                        })
                
                elif transform_type == "Cast Type":
                    data_type = st.selectbox(
                        "Data type",
                        options=["int", "float", "str"],
                        key=f"cast_{target_table}_{target_col}"
                    )
                    transformations.append({
                        'type': 'type_cast',
                        'column': source_col,
                        'dtype': data_type,
                        'target_column': target_col
                    })
        
        if st.form_submit_button("💾 Save Transformations", type="secondary"):
            if transformations:
                st.success(f"✅ {len(transformations)} transformations configured!")
            else:
                st.info("ℹ️ No transformations configured")
    
    return transformations

def show_data_preview(source_table):
    """Show data preview for a source table"""
    try:
        query = f"SELECT * FROM {escape_table_name(source_table)} LIMIT 5"
        df_preview = pd.read_sql(query, st.session_state.source_engine)
        
        if not df_preview.empty:
            st.dataframe(df_preview, use_container_width=True)
        else:
            st.info("Table is empty")
            
    except Exception as e:
        st.error(f"Preview error: {e}")

def show_export_summary():
    """Show summary of all exports"""
    st.header("📋 Export Summary")
    
    csv_paths = st.session_state.get('csv_paths', {})
    column_mappings = st.session_state.get('column_mappings', {})
    
    if not csv_paths:
        st.info("No exports completed yet.")
        return
    
    # Summary metrics
    total_exported = len(csv_paths)
    total_mapped = len([t for t in column_mappings.values() if t])  # Only count tables with actual mappings
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Tables Exported", total_exported)
    with col2:
        st.metric("Tables Mapped", total_mapped)
    with col3:
        completion = f"{(total_exported/total_mapped*100):.0f}%" if total_mapped > 0 else "0%"
        st.metric("Completion", completion)
    
    # Export details
    st.subheader("📄 Exported Files")
    for table_name, csv_path in csv_paths.items():
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            st.write(f"**{table_name}.csv**")
            mapped_cols = len(column_mappings.get(table_name, {}))
            st.caption(f"{mapped_cols} columns mapped")
        
        with col2:
            try:
                file_size = os.path.getsize(csv_path) / 1024  # KB
                row_count = sum(1 for _ in open(csv_path)) - 1  # Subtract header
                st.write(f"{row_count:,} rows")
                st.caption(f"Size: {file_size:.1f} KB")
            except:
                st.caption("File info unavailable")
        
        with col3:
            try:
                with open(csv_path, 'rb') as f:
                    st.download_button(
                        label="⬇️",
                        data=f.read(),
                        file_name=f"{table_name}.csv",
                        mime="text/csv",
                        key=f"final_download_{table_name}"
                    )
            except Exception as e:
                st.error("❌")
    
    # Final completion
    if total_exported == total_mapped and total_exported > 0:
        st.success("🎉 All tables exported successfully! Ready for analysis.")
        
        # Download all as ZIP option
        if st.button("📦 Download All Files as ZIP", type="primary"):
            create_zip_download(csv_paths)

def create_zip_download(csv_paths):
    """Create a ZIP file with all exported CSVs"""
    import zipfile
    import io
    
    try:
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for table_name, csv_path in csv_paths.items():
                zip_file.write(csv_path, f"{table_name}.csv")
        
        zip_buffer.seek(0)
        
        st.download_button(
            label="⬇️ Download All Exports (ZIP)",
            data=zip_buffer.getvalue(),
            file_name="retail_data_exports.zip",
            mime="application/zip"
        )
        
    except Exception as e:
        st.error(f"Error creating ZIP: {e}")