# utils/db_utils.py
import sqlite3
from sqlalchemy import create_engine, inspect
import mysql.connector
import psycopg2
from urllib.parse import quote_plus
import logging
import streamlit as st

logger = logging.getLogger(__name__)

def create_connection(db_type, db_params):
    try:
        if db_type == "sqlite":
            engine = create_engine(f"sqlite:///{db_params['db_path']}")
            conn = sqlite3.connect(db_params['db_path'])
        elif db_type == "postgresql":
            conn_str = (
                f"postgresql://{db_params['user']}:{quote_plus(db_params['password'])}@"
                f"{db_params['host']}:{db_params['port']}/{db_params['database']}"
            )
            engine = create_engine(conn_str)
            conn = psycopg2.connect(
                dbname=db_params['database'],
                user=db_params['user'],
                password=db_params['password'],
                host=db_params['host'],
                port=db_params['port']
            )
        elif db_type == "mysql":
            conn_str = (
                f"mysql+mysqlconnector://{db_params['user']}:{quote_plus(db_params['password'])}@"
                f"{db_params['host']}:{db_params['port']}/{db_params['database']}"
            )
            engine = create_engine(conn_str)
            conn = mysql.connector.connect(
                user=db_params['user'],
                password=db_params['password'],
                host=db_params['host'],
                port=db_params['port'],
                database=db_params['database']
            )
        else:
            raise ValueError("Unsupported database type")
        return conn, engine
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        st.error(f"Failed to connect to database: {e}")
        return None, None

def get_table_info(engine):
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        table_columns = {table: [col['name'] for col in inspector.get_columns(table)] for table in tables}
        return tables, table_columns
    except Exception as e:
        logger.error(f"Error getting table info: {e}")
        st.error(f"Error getting table info: {e}")
        return [], {}

def escape_table_name(table_name):
    return f'"{table_name}"'