# utils/etl_utils.py
import pandas as pd
from sqlalchemy import inspect
import streamlit as st
import logging

logger = logging.getLogger(__name__)

def transform_and_export(engine, table_name, columns, target_columns, output_path, transformations=None, chunk_size=10000):
    try:
        if not columns:
            st.warning(f"No columns mapped for table '{table_name}'. Skipping export.")
            logger.warning(f"No columns mapped for table '{table_name}'.")
            return False

        inspector = inspect(engine)
        actual_columns = [col['name'] for col in inspector.get_columns(table_name)]
        
        valid_columns = []
        valid_target_columns = []
        for src_col, tgt_col in zip(columns, target_columns):
            if src_col in actual_columns:
                valid_columns.append(src_col)
                valid_target_columns.append(tgt_col)
            else:
                st.warning(f"Column '{src_col}' not found in source table '{table_name}'. Skipping.")
                logger.warning(f"Column '{src_col}' not found in source table '{table_name}'.")

        if not valid_columns:
            st.warning(f"No valid columns to export for table '{table_name}'. Skipping.")
            logger.warning(f"No valid columns to export for table '{table_name}'.")
            return False

        query = f"SELECT {', '.join([f'[{col}]' for col in valid_columns])} FROM {table_name}"
        
        chunks = pd.read_sql(query, engine, chunksize=chunk_size)
        first_chunk = True
        
        for df in chunks:
            if df.empty:
                st.warning(f"Table '{table_name}' is empty. Creating empty CSV.")
                logger.warning(f"Table '{table_name}' is empty.")
                df.to_csv(output_path, mode='w', index=False, header=valid_target_columns)
                return True

            if transformations:
                for transform in transformations:
                    try:
                        if transform['type'] == 'fill_na':
                            if transform['column'] in df.columns:
                                df[transform['column']] = df[transform['column']].fillna(transform['value'])
                            else:
                                logger.warning(f"Transformation column '{transform['column']}' not in dataframe for '{table_name}'.")
                        elif transform['type'] == 'type_cast':
                            if transform['column'] in df.columns:
                                df[transform['column']] = df[transform['column']].astype(transform['dtype'])
                            else:
                                logger.warning(f"Transformation column '{transform['column']}' not in dataframe for '{table_name}'.")
                    except Exception as e:
                        logger.warning(f"Transformation error for {table_name}: {e}")
                        st.warning(f"Transformation error: {e}")

            df.columns = valid_target_columns
            mode = 'w' if first_chunk else 'a'
            df.to_csv(output_path, mode=mode, index=False, header=first_chunk)
            first_chunk = False

        logger.info(f"Exported table '{table_name}' to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error exporting table '{table_name}': {e}")
        st.error(f"Error exporting table '{table_name}': {e}")
        return False