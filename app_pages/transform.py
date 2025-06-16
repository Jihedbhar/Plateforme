# pages/transform.py
import streamlit as st
import pandas as pd
import os
import json
from utils.db_utils import get_table_info, escape_table_name
from utils.etl_utils import transform_and_export
from utils.config import TEMP_DIR, MAPPINGS_DIR, DEFAULT_COLUMN_MAPPINGS
import logging

logger = logging.getLogger(__name__)

def transform_page():
    if 'source_engine' not in st.session_state or not st.session_state.source_engine:
        st.error("⚠️ Please connect to a source database first.")
        return
    if 'target_engine' not in st.session_state or not st.session_state.target_engine:
        st.error("⚠️ Please connect to a target database first.")
        return
    if not st.session_state.table_mapping:
        st.error("⚠️ Please complete table mapping first.")
        return

    st.header("📊 Column Mapping & Transformations")
    logger.info(f"Table mappings in transform: {st.session_state.table_mapping}")
    logger.info(f"CSV paths before transform: {st.session_state.csv_paths}")

    source_tables, source_table_columns = get_table_info(st.session_state.source_engine)
    target_tables, target_table_columns = get_table_info(st.session_state.target_engine)
    column_mappings = {}
    csv_paths = {}
    transformations = {}

    for target_table, source_table in st.session_state.table_mapping.items():
        if source_table == 'None':
            logger.warning(f"Skipping target table '{target_table}' as it is mapped to 'None'.")
            continue
        with st.expander(f"🔧 Configure {target_table} (← {source_table})", expanded=True):
            target_columns = target_table_columns[target_table]
            source_columns = source_table_columns[source_table]

            st.write(f"**Target columns:** {', '.join(target_columns)}")
            st.write(f"**Source columns:** {', '.join(source_columns)}")

            column_mapping = {}
            default_cols = DEFAULT_COLUMN_MAPPINGS.get(target_table, {})

            # Column Mapping
            st.subheader("Column Mapping")
            col_map_cols = st.columns(2)
            for i, target_col in enumerate(target_columns):
                col_idx = i % 2
                with col_map_cols[col_idx]:
                    default_mapped_col = default_cols.get(target_col, 'None')
                    valid_col_options = ['None'] + source_columns
                    default_col_index = valid_col_options.index(default_mapped_col) if default_mapped_col in valid_col_options else 0
                    selected_col = st.selectbox(
                        f"{target_col}:",
                        options=valid_col_options,
                        index=default_col_index,
                        key=f"col_map_{target_table}_{target_col}"
                    )
                    if selected_col == 'None':
                        st.warning(f"⚠️ Target column '{target_col}' in '{target_table}' not mapped. It will be excluded.")
                    else:
                        column_mapping[target_col] = selected_col

            # Transformations
            if column_mapping:
                st.subheader("Transformations")
                st.markdown(f"Configure transformations for columns in **{target_table}**")
                
                with st.form(key=f"transform_form_{target_table}"):
                    transform_configs = []
                    for target_col in column_mapping:
                        st.markdown(f"**Transform {target_col}**")
                        col1, col2 = st.columns([1, 2])
                        
                        with col1:
                            transform_type = st.selectbox(
                                "Transformation Type",
                                options=["None", "Fill NA", "Type Cast"],
                                key=f"transform_type_{target_table}_{target_col}"
                            )
                        
                        with col2:
                            if transform_type == "Fill NA":
                                fill_value = st.text_input(
                                    "Fill value",
                                    key=f"fill_value_{target_table}_{target_col}",
                                    placeholder="e.g., 0 or Unknown"
                                )
                                if fill_value and column_mapping[target_col] in source_columns:
                                    transform_configs.append({
                                        'type': 'fill_na',
                                        'column': column_mapping[target_col],
                                        'value': fill_value
                                    })
                                elif fill_value:
                                    st.warning(f"Cannot apply Fill NA to '{target_col}' (source column '{column_mapping[target_col]}' not found).")
                            elif transform_type == "Type Cast":
                                dtype = st.selectbox(
                                    "Data type",
                                    options=["int", "float", "str"],
                                    key=f"dtype_{target_table}_{target_col}"
                                )
                                if column_mapping[target_col] in source_columns:
                                    transform_configs.append({
                                        'type': 'type_cast',
                                        'column': column_mapping[target_col],
                                        'dtype': dtype
                                    })
                                else:
                                    st.warning(f"Cannot apply Type Cast to '{target_col}' (source column '{column_mapping[target_col]}' not found).")
                    
                    if transform_configs:
                        transformations[target_table] = transform_configs
                    
                    if st.form_submit_button(f"Apply Transformations for {target_table}"):
                        st.success(f"Transformations configured for {target_table}")

            # Export to CSV
            if column_mapping:
                column_mappings[target_table] = column_mapping
                csv_path = os.path.join(TEMP_DIR, f"{target_table}.csv")
                with st.spinner(f"Exporting {target_table}..."):
                    if transform_and_export(
                        st.session_state.source_engine,
                        source_table,
                        list(column_mapping.values()),
                        list(column_mapping.keys()),
                        csv_path,
                        transformations.get(target_table, [])
                    ):
                        csv_paths[target_table] = csv_path
                        st.success(f"✅ Exported {target_table} to CSV at {csv_path}")
                        logger.info(f"Exported {target_table} to {csv_path}")
                        if target_table == 'Transactions':
                            try:
                                df = pd.read_csv(csv_path, nrows=5)
                                logger.info(f"Transactions CSV preview: {df.columns.tolist()}")
                                st.write(f"Transactions CSV preview (first 5 rows):")
                                st.dataframe(df)
                            except Exception as e:
                                logger.error(f"Error previewing Transactions CSV: {e}")
                                st.error(f"Error previewing Transactions CSV: {e}")
                    else:
                        logger.error(f"Failed to export {target_table}")
                        st.error(f"Failed to export {target_table}")

            # Data preview
            if st.checkbox(f"Preview {source_table}", key=f"preview_{source_table}"):
                query = f"SELECT * FROM {escape_table_name(source_table)} LIMIT 5"
                df_preview = pd.read_sql(query, st.session_state.source_engine)
                st.dataframe(df_preview, use_container_width=True)

    # Save and Proceed
    if column_mappings:
        st.header("💾 Save & Export")
        if st.button("🚀 Save Mappings and Proceed", type="primary"):
            st.session_state.column_mappings = column_mappings
            st.session_state.csv_paths = csv_paths
            
            # Save mappings to disk
            mappings_path = os.path.join(MAPPINGS_DIR, "mappings.json")
            with open(mappings_path, "w") as f:
                json.dump({
                    "table_mapping": st.session_state.table_mapping,
                    "column_mappings": column_mappings
                }, f)
            st.session_state.mappings_path = mappings_path
            
            st.success("🎉 ETL pipeline completed! CSVs and mappings saved.")
            st.balloons()
            st.json({"table_mapping": st.session_state.table_mapping, "column_mappings": column_mappings})
            logger.info(f"Saved mappings to {mappings_path}")
            logger.info(f"Final CSV paths: {csv_paths}")

    # Clean up
    if st.session_state.source_conn:
        try:
            st.session_state.source_conn.close()
        except:
            pass
        st.session_state.source_engine.dispose()
    if st.session_state.target_conn:
        try:
            st.session_state.target_conn.close()
        except:
            pass
        st.session_state.target_engine.dispose()