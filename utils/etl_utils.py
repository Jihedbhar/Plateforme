# utils/etl_utils.py
import pandas as pd
from sqlalchemy import inspect, text
import streamlit as st
import logging

logger = logging.getLogger(__name__)

def transform_and_export(engine, table_name, columns, target_columns, output_path, transformations=None, chunk_size=10000):
    """
    Transform and export data from source table to CSV with optional transformations.
    
    Args:
        engine: SQLAlchemy engine for database connection
        table_name: Name of source table
        columns: List of source column names to extract
        target_columns: List of target column names for output
        output_path: Path where CSV file will be saved
        transformations: Optional list of transformation configurations
        chunk_size: Number of rows to process at once
    
    Returns:
        bool: True if export successful, False otherwise
    """
    try:
        # Validate inputs
        if not columns:
            st.warning(f"No columns mapped for table '{table_name}'. Skipping export.")
            logger.warning(f"No columns mapped for table '{table_name}'.")
            return False

        if len(columns) != len(target_columns):
            st.error(f"Mismatch between source columns ({len(columns)}) and target columns ({len(target_columns)})")
            logger.error(f"Column count mismatch for table '{table_name}'")
            return False

        # Inspect table structure
        inspector = inspect(engine)
        actual_columns = [col['name'] for col in inspector.get_columns(table_name)]
        
        # Validate columns exist in source table
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

        # Build query with proper column escaping
        escaped_columns = []
        for col in valid_columns:
            # Handle different database types
            if engine.dialect.name == 'sqlite':
                escaped_columns.append(f'`{col}`')
            elif engine.dialect.name == 'mysql':
                escaped_columns.append(f'`{col}`')
            elif engine.dialect.name == 'postgresql':
                escaped_columns.append(f'"{col}"')
            else:
                escaped_columns.append(f'[{col}]')  # SQL Server style
        
        # Escape table name too
        if engine.dialect.name == 'sqlite':
            escaped_table = f'`{table_name}`'
        elif engine.dialect.name == 'mysql':
            escaped_table = f'`{table_name}`'
        elif engine.dialect.name == 'postgresql':
            escaped_table = f'"{table_name}"'
        else:
            escaped_table = f'[{table_name}]'
        
        query = f"SELECT {', '.join(escaped_columns)} FROM {escaped_table}"
        logger.info(f"Executing query: {query}")
        
        # Process data in chunks
        try:
            chunks = pd.read_sql(query, engine, chunksize=chunk_size)
        except Exception as e:
            # Try with text() wrapper for SQLAlchemy 2.0 compatibility
            chunks = pd.read_sql(text(query), engine, chunksize=chunk_size)
        
        first_chunk = True
        total_rows = 0
        
        for df in chunks:
            if df.empty:
                if first_chunk:  # Only show warning for completely empty table
                    st.warning(f"Table '{table_name}' is empty. Creating empty CSV.")
                    logger.warning(f"Table '{table_name}' is empty.")
                    # Create empty CSV with headers
                    empty_df = pd.DataFrame(columns=valid_target_columns)
                    empty_df.to_csv(output_path, index=False)
                    return True
                continue

            # Apply transformations if specified
            if transformations:
                df = apply_transformations(df, transformations, table_name)

            # Rename columns to target names
            df.columns = valid_target_columns
            
            # Write to CSV
            mode = 'w' if first_chunk else 'a'
            df.to_csv(output_path, mode=mode, index=False, header=first_chunk)
            
            total_rows += len(df)
            first_chunk = False

        if total_rows == 0:
            st.warning(f"Table '{table_name}' is empty. Creating empty CSV.")
            empty_df = pd.DataFrame(columns=valid_target_columns)
            empty_df.to_csv(output_path, index=False)
        else:
            logger.info(f"Exported {total_rows} rows from table '{table_name}' to {output_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error exporting table '{table_name}': {e}")
        st.error(f"Error exporting table '{table_name}': {e}")
        return False

def apply_transformations(df, transformations, table_name):
    """
    Apply transformations to a DataFrame.
    
    Args:
        df: Pandas DataFrame to transform
        transformations: List of transformation configurations
        table_name: Name of table (for logging)
    
    Returns:
        DataFrame: Transformed DataFrame
    """
    df_transformed = df.copy()
    
    for transform in transformations:
        try:
            transform_type = transform.get('type')
            column = transform.get('column')
            
            if column not in df_transformed.columns:
                logger.warning(f"Transformation column '{column}' not in dataframe for '{table_name}'.")
                continue
            
            if transform_type == 'fill_na':
                fill_value = transform.get('value', '')
                original_nulls = df_transformed[column].isnull().sum()
                df_transformed[column] = df_transformed[column].fillna(fill_value)
                logger.info(f"Filled {original_nulls} null values in '{column}' with '{fill_value}'")
                
            elif transform_type == 'type_cast':
                dtype = transform.get('dtype', 'str')
                original_dtype = df_transformed[column].dtype
                
                # Handle type casting more safely
                if dtype == 'int':
                    # Convert to numeric first, then to int (handles strings better)
                    df_transformed[column] = pd.to_numeric(df_transformed[column], errors='coerce').astype('Int64')
                elif dtype == 'float':
                    df_transformed[column] = pd.to_numeric(df_transformed[column], errors='coerce')
                elif dtype == 'str':
                    df_transformed[column] = df_transformed[column].astype(str)
                
                logger.info(f"Cast column '{column}' from {original_dtype} to {dtype}")
                
            else:
                logger.warning(f"Unknown transformation type: {transform_type}")
                
        except Exception as e:
            logger.warning(f"Transformation error for column '{column}' in {table_name}: {e}")
            st.warning(f"Transformation error for column '{column}': {e}")
    
    return df_transformed

def get_table_row_count(engine, table_name):
    """
    Get the number of rows in a table.
    
    Args:
        engine: SQLAlchemy engine
        table_name: Name of table
        
    Returns:
        int: Number of rows, or 0 if error
    """
    try:
        # Escape table name based on database type
        if engine.dialect.name == 'sqlite':
            escaped_table = f'`{table_name}`'
        elif engine.dialect.name == 'mysql':
            escaped_table = f'`{table_name}`'
        elif engine.dialect.name == 'postgresql':
            escaped_table = f'"{table_name}"'
        else:
            escaped_table = f'[{table_name}]'
            
        query = f"SELECT COUNT(*) as row_count FROM {escaped_table}"
        
        try:
            result = pd.read_sql(query, engine)
        except:
            result = pd.read_sql(text(query), engine)
            
        return result['row_count'].iloc[0]
    except Exception as e:
        logger.error(f"Error getting row count for {table_name}: {e}")
        return 0

def validate_export_file(file_path, expected_columns):
    """
    Validate that an exported CSV file has the expected structure.
    
    Args:
        file_path: Path to CSV file
        expected_columns: List of expected column names
        
    Returns:
        dict: Validation results with 'valid', 'message', 'row_count', 'column_count'
    """
    try:
        # Read just the header and first few rows
        df = pd.read_csv(file_path, nrows=5)
        
        actual_columns = list(df.columns)
        row_count = len(pd.read_csv(file_path))  # Get full row count
        
        # Check columns match
        if set(actual_columns) == set(expected_columns):
            return {
                'valid': True,
                'message': f'File valid: {row_count} rows, {len(actual_columns)} columns',
                'row_count': row_count,
                'column_count': len(actual_columns)
            }
        else:
            missing = set(expected_columns) - set(actual_columns)
            extra = set(actual_columns) - set(expected_columns)
            message = f"Column mismatch. Missing: {missing}, Extra: {extra}"
            return {
                'valid': False,
                'message': message,
                'row_count': row_count,
                'column_count': len(actual_columns)
            }
            
    except Exception as e:
        return {
            'valid': False,
            'message': f'Error reading file: {e}',
            'row_count': 0,
            'column_count': 0
        }