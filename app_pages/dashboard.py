# pages/dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import os
import logging
from utils.db_utils import create_connection
from utils.config import TEMP_DIR, DB_TYPES
from itertools import combinations

logger = logging.getLogger(__name__)

def dashboard_page():
    st.title("📊 B2C Retailer Performance Dashboard")
    st.markdown("**Unlock powerful insights to maximize sales and profits with data-driven decisions.**")

    # Debug session state
    logger.info(f"Session state in dashboard: {st.session_state}")
    if st.session_state.get('db_type') is None and st.session_state.get('db_path'):
        st.session_state.db_type = 'sqlite'
    elif st.session_state.get('db_type') is None and st.session_state.get('db_params'):
        st.session_state.db_type = 'postgresql' if st.session_state.db_params.get('port') == "5432" else 'mysql'
    st.sidebar.write("### 🛠 Debug: Session State")
    st.sidebar.json(st.session_state)

    # Reconnect to database if needed
    def reconnect_database():
        if st.session_state.get('db_type') and st.session_state.get('db_params'):
            try:
                conn, engine = create_connection(DB_TYPES[st.session_state.db_type], st.session_state.db_params)
                if conn and engine:
                    st.session_state.source_engine = engine
                    st.session_state.source_conn = conn
                    logger.info(f"Reconnected to {st.session_state.db_type} database")
                    return True
                else:
                    logger.error("Failed to reconnect to database")
                    st.error("Failed to reconnect to database")
                    return False
            except Exception as e:
                logger.error(f"Error reconnecting to database: {e}")
                st.error(f"Error reconnecting to database: {e}")
                return False
        else:
            logger.warning("No valid database configuration found for reconnection")
            return False

    # Set db_params for SQLite
    if st.session_state.get('db_type') == 'sqlite' and st.session_state.get('db_path'):
        st.session_state.db_params = {'db_path': st.session_state.db_path}

    # Check if mappings and CSVs exist
    if not st.session_state.get('table_mapping') or not st.session_state.get('csv_paths'):
        st.error("🚫 **Setup Required:** Please complete the setup and transformation steps.")
        st.markdown("### 👈 Use the sidebar to navigate to the 'Setup' page.")
        st.stop()
    elif not st.session_state.get('source_engine'):
        if not reconnect_database():
            st.error("🚫 **Database Error:** Could not reconnect to the database. Please return to the 'Setup' page.")
            st.stop()

    # Check Transactions specifically
    if 'Transactions' not in st.session_state.csv_paths:
        st.error("⚠️ Transactions table CSV not found. Please ensure it was exported in the Transform & Export step.")
        logger.error("Transactions table missing in csv_paths")
        st.stop()
    else:
        trans_csv = st.session_state.csv_paths['Transactions']
        if not os.path.exists(trans_csv):
            st.error(f"⚠️ Transactions CSV file not found at {trans_csv}.")
            logger.error(f"Transactions CSV missing at {trans_csv}")
            st.stop()
        else:
            try:
                df_trans_preview = pd.read_csv(trans_csv, nrows=5)
                logger.info(f"Transactions CSV columns: {df_trans_preview.columns.tolist()}")
                st.write("Transactions CSV preview (first 5 rows):")
                st.dataframe(df_trans_preview)
            except Exception as e:
                logger.error(f"Error reading Transactions CSV: {e}")
                st.error(f"Error reading Transactions CSV: {e}")

    # Helper function to safely read CSV
    @st.cache_data
    def safe_read_csv(table_name, usecols=None):
        try:
            if table_name in st.session_state.csv_paths:
                csv_path = st.session_state.csv_paths[table_name]
                if not os.path.exists(csv_path):
                    st.warning(f"⚠️ CSV file for {table_name} not found at {csv_path}.")
                    logger.warning(f"CSV file for {table_name} missing at {csv_path}")
                    return pd.DataFrame()
                df = pd.read_csv(csv_path, usecols=usecols)
                if df.empty:
                    st.warning(f"⚠️ {table_name} CSV is empty.")
                    logger.warning(f"{table_name} CSV is empty")
                    return pd.DataFrame()
                logger.info(f"Successfully read {table_name} CSV with columns: {df.columns.tolist()}")
                return df
            else:
                st.warning(f"⚠️ {table_name} CSV not found in session state.")
                logger.warning(f"{table_name} CSV not found in csv_paths")
                return pd.DataFrame()
        except Exception as e:
            st.error(f"❌ Error reading {table_name} CSV: {e}")
            logger.error(f"Error reading {table_name} CSV: {e}")
            return pd.DataFrame()

    # Helper function to check required columns
    def check_columns(df, required_cols, table_name, optional_cols=None):
        if df.empty:
            st.warning(f"⚠️ {table_name} data is empty or not available.")
            logger.warning(f"{table_name} data is empty")
            return False, []
        
        # Get column mappings for the table
        mappings = st.session_state.column_mappings.get(table_name, {})
        logger.info(f"Column mappings for {table_name}: {mappings}")
        # Map target columns to source columns
        source_required_cols = [mappings.get(col, col) for col in required_cols]
        source_optional_cols = [mappings.get(col, col) for col in optional_cols or []]
        
        missing_required = [col for col in source_required_cols if col not in df.columns]
        available_optional = [col for col in source_optional_cols if col in df.columns]
        if missing_required:
            st.warning(f"⚠️ Missing required columns in {table_name}: {missing_required}")
            logger.warning(f"Missing required columns in {table_name}: {missing_required}")
            return False, available_optional
        return True, available_optional

    # Display database status in sidebar
    with st.sidebar:
        st.markdown("### 📋 Database & Dashboard Status")
        if st.session_state.get('db_type') == 'sqlite' and st.session_state.get('db_path'):
            st.write(f"📂 Database: {os.path.basename(st.session_state.db_path)} (SQLite)")
        elif st.session_state.get('db_type') in ['postgresql', 'mysql'] and st.session_state.get('db_params'):
            st.write(f"📂 Database: {st.session_state.db_params.get('database', 'Unknown')} ({st.session_state.db_type})")
        else:
            st.write("⚠️ Database not fully configured")
        
        st.write("**Available Tables:**")
        available_tables = list(st.session_state.csv_paths.keys())
        for table in ['Transactions', 'Produit', 'Client', 'Stock', 'Magasin', 'Localisation', 'Employé']:
            status = "✅" if table in available_tables else "❌"
            st.write(f"{status} {table}")

    # Create dashboard layout
    st.markdown("### 🎯 Key Insights Overview")

    # Create tabs for different analysis sections
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Sales Analytics",
        "👥 Customer Insights",
        "📦 Inventory Management",
        "🏪 Store Performance",
        "🤝 Cross-Selling & Profitability"
    ])

    with tab1:
        st.header("📈 Sales Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("💰 Sales Trend Over Time")
            df_trans = safe_read_csv('Transactions')
            
            mappings = st.session_state.column_mappings.get('Transactions', {})
            date_col = mappings.get('date_heure', 'date_heure')
            sales_col = mappings.get('montant_total', 'montant_total')
            required_cols = [date_col, sales_col]
            optional_cols = [mappings.get('id_client', 'id_client'), mappings.get('id_produit', 'id_produit')]
            has_required, available_optional = check_columns(df_trans, required_cols, 'Transactions', optional_cols)
            
            if has_required:
                try:
                    df_trans[date_col] = pd.to_datetime(df_trans[date_col], errors='coerce')
                    df_clean = df_trans.dropna(subset=[date_col, sales_col])
                    
                    if not df_clean.empty:
                        df_clean['month'] = df_clean[date_col].dt.to_period('M').astype(str)
                        sales_trend = df_clean.groupby('month')[sales_col].sum().reset_index()
                        
                        fig = px.line(
                            sales_trend,
                            x='month',
                            y=sales_col,
                            title='Monthly Sales Trend',
                            labels={'month': 'Month', sales_col: 'Total Sales (TND)'},
                            color_discrete_sequence=['#1f77b4']
                        )
                        fig.update_layout(xaxis_tickangle=45, height=400)
                        st.plotly_chart(fig, use_container_width=True)
                        
                        max_sales_month = sales_trend.loc[sales_trend[sales_col].idxmax(), 'month']
                        st.info(f"🏆 **Peak Sales Month:** {max_sales_month}")
                        
                        total_sales = df_clean[sales_col].sum()
                        st.metric("Total Sales", f"{total_sales:,.2f} TND")
                    else:
                        st.warning("No valid date/sales data found after cleaning.")
                except Exception as e:
                    st.error(f"Error creating sales trend: {e}")
                    logger.error(f"Sales trend error: {e}")
            else:
                st.warning("Sales trend data not available.")
                if sales_col in df_trans.columns:
                    total_sales = df_trans[sales_col].sum()
                    st.metric("Total Sales (Fallback)", f"{total_sales:,.2f} TND")

        with col2:
            st.subheader("📊 Sales by Product Category")
            df_trans = safe_read_csv('Transactions')
            df_prod = safe_read_csv('Produit')
            
            trans_mappings = st.session_state.column_mappings.get('Transactions', {})
            prod_mappings = st.session_state.column_mappings.get('Produit', {})
            prod_id_col = trans_mappings.get('id_produit', 'id_produit')
            sales_col = trans_mappings.get('montant_total', 'montant_total')
            prod_id_col_prod = prod_mappings.get('id_produit', 'id_produit')
            category_col = prod_mappings.get('catégorie', 'catégorie')
            
            if (not df_trans.empty and not df_prod.empty and
                check_columns(df_trans, [prod_id_col, sales_col], 'Transactions')[0] and
                check_columns(df_prod, [prod_id_col_prod, category_col], 'Produit')[0]):
                try:
                    df = df_trans.merge(df_prod[[prod_id_col_prod, category_col]], left_on=prod_id_col, right_on=prod_id_col_prod, how='left')
                    sales_by_category = df.groupby(category_col)[sales_col].sum().reset_index()
                    sales_by_category = sales_by_category.sort_values(sales_col, ascending=True)
                    
                    fig = px.bar(
                        sales_by_category,
                        x=sales_col,
                        y=category_col,
                        orientation='h',
                        title='Sales by Product Category',
                        labels={category_col: 'Product Category', sales_col: 'Total Sales (TND)'},
                        color=sales_col,
                        color_continuous_scale='viridis'
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    top_category = sales_by_category.loc[sales_by_category[sales_col].idxmax(), category_col]
                    st.info(f"🥇 **Top Category:** {top_category}")
                except Exception as e:
                    st.error(f"Error creating category analysis: {e}")
                    logger.error(f"Category analysis error: {e}")
            else:
                st.warning("Product category data not available.")

        st.subheader("🌞 Seasonal Sales Patterns")
        df_trans = safe_read_csv('Transactions')
        if not df_trans.empty and check_columns(df_trans, [date_col, sales_col], 'Transactions')[0]:
            try:
                df_clean = df_trans.copy()
                df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
                
                invalid_dates = df_clean[date_col].isna().sum()
                if invalid_dates > 0:
                    st.warning(f"⚠️ {invalid_dates} rows in '{date_col}' could not be parsed as valid dates. These rows will be excluded.")
                    logger.warning(f"{invalid_dates} invalid date_heure values in Transactions")
                
                df_clean = df_clean.dropna(subset=[date_col, sales_col])
                
                if not df_clean.empty:
                    df_clean['month_num'] = df_clean[date_col].dt.month
                    seasonal_sales = df_clean.groupby('month_num')[sales_col].mean().reset_index()
                    month_names = [
                        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                        'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
                    ]
                    seasonal_sales['month_name'] = seasonal_sales['month_num'].map(lambda x: month_names[int(x)-1])
                    
                    fig = px.bar(
                        seasonal_sales,
                        x='month_name',
                        y=sales_col,
                        title='Average Sales by Month (Seasonality)',
                        labels={'month_name': 'Month', sales_col: 'Average Sales (TND)'},
                        color=sales_col,
                        color_continuous_scale='blues'
                    )
                    fig.update_layout(height=400, xaxis={'categoryorder': 'array', 'categoryarray': month_names})
                    st.plotly_chart(fig, use_container_width=True)
                    
                    peak_month = seasonal_sales.loc[seasonal_sales[sales_col].idxmax(), 'month_name']
                    st.info(f"🌟 **Peak Sales Season:** {peak_month}")
                else:
                    st.warning("No valid data available for seasonal sales analysis after cleaning.")
            except Exception as e:
                st.error(f"Error creating seasonal sales analysis: {e}")
                logger.error(f"Seasonal sales error: {e}")
                invalid_rows = df_trans[df_trans[date_col].apply(lambda x: pd.isna(pd.to_datetime(x, errors='coerce')))]
                if not invalid_rows.empty:
                    st.write("Sample of problematic date_heure values:", invalid_rows[date_col].head())
        else:
            st.warning("Seasonal sales data not available.")

    with tab2:
        st.header("👥 Customer Insights")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🎖️ Customer Loyalty Distribution")
            df_client = safe_read_csv('Client')
            df_trans = safe_read_csv('Transactions')
            
            client_mappings = st.session_state.column_mappings.get('Client', {})
            trans_mappings = st.session_state.column_mappings.get('Transactions', {})
            client_id_col = client_mappings.get('id_client', 'id_client')
            loyalty_col = client_mappings.get('Tier_fidelité', 'Tier_fidelité')
            trans_client_id_col = trans_mappings.get('id_client', 'id_client')
            sales_col = trans_mappings.get('montant_total', 'montant_total')
            
            if (not df_client.empty and not df_trans.empty and
                check_columns(df_client, [client_id_col, loyalty_col], 'Client')[0] and
                check_columns(df_trans, [trans_client_id_col, sales_col], 'Transactions')[0]):
                try:
                    df = df_trans.merge(df_client[[client_id_col, loyalty_col]], left_on=trans_client_id_col, right_on=client_id_col, how='left')
                    loyalty_spending = df.groupby(loyalty_col)[sales_col].sum().reset_index()
                    
                    fig = px.pie(
                        loyalty_spending,
                        names=loyalty_col,
                        values=sales_col,
                        title='Customer Spending by Loyalty Tier',
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    total_customers = df_client[client_id_col].nunique()
                    st.metric("Total Customers", total_customers)
                except Exception as e:
                    st.error(f"Error creating loyalty analysis: {e}")
                    logger.error(f"Loyalty analysis error: {e}")
            else:
                st.warning("Customer loyalty data not available.")

        with col2:
            st.subheader("📍 Customer Geographic Distribution")
            df_client = safe_read_csv('Client')
            
            client_mappings = st.session_state.column_mappings.get('Client', {})
            city_col = client_mappings.get('ville', 'ville')
            
            if not df_client.empty and check_columns(df_client, [city_col], 'Client')[0]:
                try:
                    city_distribution = df_client[city_col].value_counts().reset_index()
                    city_distribution.columns = [city_col, 'count']
                    city_distribution = city_distribution.head(10)
                    
                    fig = px.bar(
                        city_distribution,
                        x='count',
                        y=city_col,
                        orientation='h',
                        title='Top 10 Cities by Customer Count',
                        labels={city_col: 'City', 'count': 'Number of Customers'},
                        color='count',
                        color_continuous_scale='blues'
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating geographic distribution: {e}")
                    logger.error(f"Geographic distribution error: {e}")
            else:
                st.warning("Customer geographic data not available.")

        st.subheader("💸 Customer Spending Segmentation")
        df_client = safe_read_csv('Client')
        df_trans = safe_read_csv('Transactions')
        
        if (not df_client.empty and not df_trans.empty and
            check_columns(df_client, [client_id_col], 'Client')[0] and
            check_columns(df_trans, [trans_client_id_col, sales_col], 'Transactions')[0]):
            try:
                customer_spending = df_trans.groupby(trans_client_id_col)[sales_col].sum().reset_index()
                bins = [0, 500, 2000, float('inf')]
                labels = ['Low Spender', 'Medium Spender', 'High Spender']
                customer_spending['segment'] = pd.cut(
                    customer_spending[sales_col],
                    bins=bins,
                    labels=labels,
                    include_lowest=True
                )
                segment_counts = customer_spending['segment'].value_counts().reset_index()
                segment_counts.columns = ['segment', 'count']
                
                fig = px.bar(
                    segment_counts,
                    x='segment',
                    y='count',
                    title='Customer Segmentation by Spending',
                    labels={'segment': 'Spending Segment', 'count': 'Number of Customers'},
                    color='count',
                    color_continuous_scale='purples'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
                
                high_spenders = segment_counts[segment_counts['segment'] == 'High Spender']['count'].iloc[0]
                st.metric("High Spenders", high_spenders)
            except Exception as e:
                st.error(f"Error creating spending segmentation: {e}")
                logger.error(f"Spending segmentation error: {e}")
        else:
            st.warning("Customer spending data not available.")

        st.subheader("🔄 Customer Purchase Frequency")
        df_trans = safe_read_csv('Transactions')
        
        date_col = trans_mappings.get('date_heure', 'date_heure')
        
        if not df_trans.empty and check_columns(df_trans, [trans_client_id_col, date_col], 'Transactions')[0]:
            try:
                df_trans[date_col] = pd.to_datetime(df_trans[date_col], errors='coerce')
                purchase_counts = df_trans.groupby(trans_client_id_col).size().reset_index(name='purchase_count')
                bins = [0, 1, 5, 10, float('inf')]
                labels = ['One-Time', 'Occasional', 'Regular', 'Frequent']
                purchase_counts['frequency'] = pd.cut(
                    purchase_counts['purchase_count'],
                    bins=bins,
                    labels=labels,
                    include_lowest=True
                )
                frequency_dist = purchase_counts['frequency'].value_counts().reset_index()
                frequency_dist.columns = ['frequency', 'count']
                
                fig = px.pie(
                    frequency_dist,
                    names='frequency',
                    values='count',
                    title='Customer Purchase Frequency Distribution',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
                
                frequent_buyers = frequency_dist[frequency_dist['frequency'] == 'Frequent']['count'].iloc[0]
                st.metric("Frequent Buyers", frequent_buyers)
            except Exception as e:
                st.error(f"Error creating purchase frequency analysis: {e}")
                logger.error(f"Purchase frequency error: {e}")
        else:
            st.warning("Purchase frequency data not available.")

    with tab3:
        st.header("📦 Inventory Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("⚠️ Low Stock Alert")
            df_stock = safe_read_csv('Stock')
            df_prod = safe_read_csv('Produit')
            
            stock_mappings = st.session_state.column_mappings.get('Stock', {})
            prod_mappings = st.session_state.column_mappings.get('Produit', {})
            stock_prod_id_col = stock_mappings.get('id_produit', 'id_produit')
            quantity_col = stock_mappings.get('quantité', 'quantité')
            threshold_col = stock_mappings.get('seuil_minimum', 'seuil_minimum')
            prod_id_col_prod = prod_mappings.get('id_produit', 'id_produit')
            prod_name_col = prod_mappings.get('nom_produit', 'nom_produit')
            
            if (not df_stock.empty and not df_prod.empty and
                check_columns(df_stock, [stock_prod_id_col, quantity_col, threshold_col], 'Stock')[0] and
                check_columns(df_prod, [prod_id_col_prod, prod_name_col], 'Produit')[0]):
                try:
                    df = df_stock.merge(df_prod[[prod_id_col_prod, prod_name_col]], left_on=stock_prod_id_col, right_on=prod_id_col_prod, how='left')
                    df_low = df[df[quantity_col] < df[threshold_col]].head(15)
                    
                    if not df_low.empty:
                        fig = go.Figure(data=[
                            go.Bar(name='Current Stock', x=df_low[prod_name_col], y=df_low[quantity_col], 
                                   marker_color='red', opacity=0.7),
                            go.Bar(name='Minimum Threshold', x=df_low[prod_name_col], y=df_low[threshold_col], 
                                   marker_color='orange', opacity=0.7)
                        ])
                        fig.update_layout(
                            barmode='group',
                            title='Products Below Stock Threshold',
                            xaxis_title='Product',
                            yaxis_title='Quantity',
                            xaxis_tickangle=45,
                            height=400
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.error(f"🚨 **{len(df_low)} products** need immediate restocking!")
                    else:
                        st.success("✅ All products are above minimum stock threshold!")
                except Exception as e:
                    st.error(f"Error creating stock analysis: {e}")
                    logger.error(f"Stock analysis error: {e}")
            else:
                st.warning("Stock data not available.")
        
        with col2:
            st.subheader("💎 Top Selling Products")
            df_trans = safe_read_csv('Transactions')
            df_prod = safe_read_csv('Produit')
            
            trans_mappings = st.session_state.column_mappings.get('Transactions', {})
            prod_mappings = st.session_state.column_mappings.get('Produit', {})
            trans_prod_id_col = trans_mappings.get('id_produit', 'id_produit')
            trans_quantity_col = trans_mappings.get('quantité', 'quantité')
            prod_id_col_prod = prod_mappings.get('id_produit', 'id_produit')
            prod_name_col = prod_mappings.get('nom_produit', 'nom_produit')
            
            if (not df_trans.empty and not df_prod.empty and
                check_columns(df_trans, [trans_prod_id_col, trans_quantity_col], 'Transactions')[0] and
                check_columns(df_prod, [prod_id_col_prod, prod_name_col], 'Produit')[0]):
                try:
                    df = df_trans.merge(df_prod[[prod_id_col_prod, prod_name_col]], left_on=trans_prod_id_col, right_on=prod_id_col_prod, how='left')
                    top_products = df.groupby(prod_name_col)[trans_quantity_col].sum().reset_index()
                    top_products = top_products.sort_values(trans_quantity_col, ascending=True).tail(10)
                    
                    fig = px.bar(
                        top_products,
                        x=trans_quantity_col,
                        y=prod_name_col,
                        orientation='h',
                        title='Top 10 Best Selling Products',
                        labels={prod_name_col: 'Product', trans_quantity_col: 'Total Quantity Sold'},
                        color=trans_quantity_col,
                        color_continuous_scale='greens'
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.error(f"Error creating top products analysis: {e}")
                    logger.error(f"Top products error: {e}")
            else:
                st.warning("Product sales data not available.")

        st.subheader("🚀 Product Sales Velocity")
        df_trans = safe_read_csv('Transactions')
        df_prod = safe_read_csv('Produit')
        
        date_col = trans_mappings.get('date_heure', 'date_heure')
        
        if (not df_trans.empty and not df_prod.empty and
            check_columns(df_trans, [trans_prod_id_col, trans_quantity_col, date_col], 'Transactions')[0] and
            check_columns(df_prod, [prod_id_col_prod, prod_name_col], 'Produit')[0]):
            try:
                df_trans[date_col] = pd.to_datetime(df_trans[date_col], errors='coerce')
                df_clean = df_trans.dropna(subset=[date_col, trans_quantity_col])
                df_clean = df_clean.merge(df_prod[[prod_id_col_prod, prod_name_col]], left_on=trans_prod_id_col, right_on=prod_id_col_prod, how='left')
                
                recent_date = df_clean[date_col].max()
                df_recent = df_clean[df_clean[date_col] >= recent_date - timedelta(days=30)]
                sales_velocity = df_recent.groupby(prod_name_col)[trans_quantity_col].sum().reset_index()
                sales_velocity['velocity'] = sales_velocity[trans_quantity_col] / 30
                sales_velocity = sales_velocity.sort_values('velocity', ascending=True).tail(10)
                
                fig = px.bar(
                    sales_velocity,
                    x='velocity',
                    y=prod_name_col,
                    orientation='h',
                    title='Top 10 Products by Sales Velocity (Last 30 Days)',
                    labels={prod_name_col: 'Product', 'velocity': 'Avg. Daily Sales'},
                    color='velocity',
                    color_continuous_scale='oranges'
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
                
                fastest_mover = sales_velocity.loc[sales_velocity['velocity'].idxmax(), prod_name_col]
                st.info(f"⚡ **Fastest Moving Product:** {fastest_mover}")
            except Exception as e:
                st.error(f"Error creating sales velocity analysis: {e}")
                logger.error(f"Sales velocity error: {e}")
        else:
            st.warning("Sales velocity data not available.")

    with tab4:
        st.header("🏪 Store Performance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🌍 Sales by Geographic Location")
            required_tables = ['Transactions', 'Magasin', 'Localisation']
            all_available = all(table in st.session_state.csv_paths for table in required_tables)
            
            if all_available:
                try:
                    df_trans = safe_read_csv('Transactions')
                    df_mag = safe_read_csv('Magasin')
                    df_loc = safe_read_csv('Localisation')
                    
                    trans_mappings = st.session_state.column_mappings.get('Transactions', {})
                    mag_mappings = st.session_state.column_mappings.get('Magasin', {})
                    loc_mappings = st.session_state.column_mappings.get('Localisation', {})
                    store_id_col = trans_mappings.get('id_magasin', 'id_magasin')
                    sales_col = trans_mappings.get('montant_total', 'montant_total')
                    mag_store_id_col = mag_mappings.get('id_magasin', 'id_magasin')
                    loc_id_col = mag_mappings.get('id_localisation', 'id_localisation')
                    loc_id_col_loc = loc_mappings.get('id_localisation', 'id_localisation')
                    city_col = loc_mappings.get('ville', 'ville')
                    
                    if (check_columns(df_trans, [store_id_col, sales_col], 'Transactions')[0] and
                        check_columns(df_mag, [mag_store_id_col, loc_id_col], 'Magasin')[0] and
                        check_columns(df_loc, [loc_id_col_loc, city_col], 'Localisation')[0]):
                        
                        df = df_trans.merge(df_mag[[mag_store_id_col, loc_id_col]], left_on=store_id_col, right_on=mag_store_id_col, how='left')
                        df = df.merge(df_loc[[loc_id_col_loc, city_col]], left_on=loc_id_col, right_on=loc_id_col_loc, how='left')
                        sales_by_city = df.groupby(city_col)[sales_col].sum().reset_index()
                        sales_by_city = sales_by_city.sort_values(sales_col, ascending=True)
                        
                        fig = px.bar(
                            sales_by_city,
                            x=sales_col,
                            y=city_col,
                            orientation='h',
                            title='Sales Performance by City',
                            labels={city_col: 'City', sales_col: 'Total Sales (TND)'},
                            color=sales_col,
                            color_continuous_scale='plasma'
                        )
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
                        
                        top_city = sales_by_city.loc[sales_by_city[sales_col].idxmax(), city_col]
                        st.info(f"🏆 **Top Performing City:** {top_city}")
                    else:
                        st.warning("Geographic sales data not available.")
                except Exception as e:
                    st.error(f"Error creating geographic sales analysis: {e}")
                    logger.error(f"Geographic sales error: {e}")
            else:
                st.warning("Required tables for geographic sales analysis not available.")

        with col2:
            st.subheader("👷 Employee Sales Performance")
            required_tables = ['Transactions', 'Employé']
            all_available = all(table in st.session_state.csv_paths for table in required_tables)
            
            if all_available:
                try:
                    df_trans = safe_read_csv('Transactions')
                    df_emp = safe_read_csv('Employé')
                    
                    trans_mappings = st.session_state.column_mappings.get('Transactions', {})
                    emp_mappings = st.session_state.column_mappings.get('Employé', {})
                    emp_id_col = trans_mappings.get('id_employé', 'id_employé')
                    sales_col = trans_mappings.get('montant_total', 'montant_total')
                    emp_id_col_emp = emp_mappings.get('id_employé', 'id_employé')
                    position_col = emp_mappings.get('poste', 'poste')
                    
                    if (check_columns(df_trans, [emp_id_col, sales_col], 'Transactions')[0] and
                        check_columns(df_emp, [emp_id_col_emp, position_col], 'Employé')[0]):
                        
                        df = df_trans.merge(df_emp[[emp_id_col_emp, position_col]], left_on=emp_id_col, right_on=emp_id_col_emp, how='left')
                        emp_sales = df.groupby(emp_id_col)[sales_col].sum().reset_index()
                        emp_sales = emp_sales.merge(df_emp[[emp_id_col_emp, position_col]], left_on=emp_id_col, right_on=emp_id_col_emp, how='left')
                        emp_sales = emp_sales.sort_values(sales_col, ascending=True).tail(10)
                        
                        fig = px.bar(
                            emp_sales,
                            x=sales_col,
                            y=emp_id_col,
                            orientation='h',
                            title='Top 10 Employees by Sales',
                            labels={emp_id_col: 'Employee ID', sales_col: 'Total Sales (TND)'},
                            color=sales_col,
                            color_continuous_scale='teal'
                        )
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
                        
                        top_employee = emp_sales.loc[emp_sales[sales_col].idxmax(), emp_id_col]
                        st.info(f"🌟 **Top Performing Employee:** ID {top_employee}")
                    else:
                        st.warning("Employee sales data not available.")
                except Exception as e:
                    st.error(f"Error creating employee sales analysis: {e}")
                    logger.error(f"Employee sales error: {e}")
            else:
                st.warning("Required tables for employee sales analysis not available.")

    with tab5:
        st.header("🤝 Cross-Selling & Profitability")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("💰 Product Profit Margins")
            df_prod = safe_read_csv('Produit')
            df_trans = safe_read_csv('Transactions')
            
            prod_mappings = st.session_state.column_mappings.get('Produit', {})
            trans_mappings = st.session_state.column_mappings.get('Transactions', {})
            prod_id_col_prod = prod_mappings.get('id_produit', 'id_produit')
            sale_price_col = prod_mappings.get('prix_vente', 'prix_vente')
            purchase_price_col = prod_mappings.get('prix_achat', 'prix_achat')
            prod_name_col = prod_mappings.get('nom_produit', 'nom_produit')
            trans_prod_id_col = trans_mappings.get('id_produit', 'id_produit')
            trans_quantity_col = trans_mappings.get('quantité', 'quantité')
            
            if (not df_prod.empty and not df_trans.empty and
                check_columns(df_prod, [prod_id_col_prod, sale_price_col, purchase_price_col], 'Produit')[0] and
                check_columns(df_trans, [trans_prod_id_col, trans_quantity_col], 'Transactions')[0]):
                try:
                    df = df_trans.merge(
                        df_prod[[prod_id_col_prod, prod_name_col, sale_price_col, purchase_price_col]],
                        left_on=trans_prod_id_col,
                        right_on=prod_id_col_prod,
                        how='left'
                    )
                    
                    df = df.dropna(subset=[sale_price_col, purchase_price_col, trans_quantity_col, prod_name_col])
                    
                    if not df.empty:
                        df['profit'] = (df[sale_price_col] - df[purchase_price_col]) * df[trans_quantity_col]
                        
                        profit_by_product = df.groupby(prod_name_col).agg({
                            'profit': 'sum',
                            sale_price_col: lambda x: (x * df.loc[x.index, trans_quantity_col]).sum()
                        }).reset_index()
                        
                        profit_by_product['margin'] = profit_by_product['profit'] / profit_by_product[sale_price_col]
                        
                        profit_by_product = profit_by_product[
                            (profit_by_product[sale_price_col] > 0) &
                            (profit_by_product['margin'].notna()) &
                            (profit_by_product['margin'].replace([np.inf, -np.inf], np.nan).notna())
                        ]
                        
                        if not profit_by_product.empty:
                            profit_by_product = profit_by_product.sort_values('margin', ascending=False).head(10)
                            
                            fig = px.bar(
                                profit_by_product,
                                x='margin',
                                y=prod_name_col,
                                orientation='h',
                                title='Top 10 Products by Profit Margin',
                                labels={prod_name_col: 'Product', 'margin': 'Profit Margin'},
                                color='margin',
                                color_continuous_scale='reds'
                            )
                            fig.update_layout(height=400)
                            st.plotly_chart(fig, use_container_width=True)
                            
                            top_margin_product = profit_by_product.loc[profit_by_product['margin'].idxmax(), prod_name_col]
                            st.info(f"💎 **Highest Margin Product:** {top_margin_product}")
                        else:
                            st.warning("No valid profit margin data after filtering invalid margins.")
                    else:
                        st.warning("No valid data after cleaning for profit margin analysis.")
                except Exception as e:
                    st.error(f"Error creating profit margin analysis: {e}")
                    logger.error(f"Profit margin error: {e}")
                    invalid_rows = df[df[[sale_price_col, purchase_price_col, trans_quantity_col]].isna().any(axis=1)]
                    if not invalid_rows.empty:
                        st.write("Sample of problematic rows:", invalid_rows[[prod_name_col, sale_price_col, purchase_price_col, trans_quantity_col]].head())
            else:
                st.warning("Profit margin data not available.")

        with col2:
            st.subheader("🤝 Cross-Selling Opportunities")
            df_trans = safe_read_csv('Transactions')
            df_prod = safe_read_csv('Produit')
            
            trans_mappings = st.session_state.column_mappings.get('Transactions', {})
            prod_mappings = st.session_state.column_mappings.get('Produit', {})
            trans_id_col = trans_mappings.get('id_transaction', 'id_transaction')
            trans_prod_id_col = trans_mappings.get('id_produit', 'id_produit')
            prod_id_col_prod = prod_mappings.get('id_produit', 'id_produit')
            prod_name_col = prod_mappings.get('nom_produit', 'nom_produit')
            
            if (not df_trans.empty and not df_prod.empty and
                check_columns(df_trans, [trans_id_col, trans_prod_id_col], 'Transactions')[0] and
                check_columns(df_prod, [prod_id_col_prod, prod_name_col], 'Produit')[0]):
                try:
                    df = df_trans.merge(df_prod[[prod_id_col_prod, prod_name_col]], left_on=trans_prod_id_col, right_on=prod_id_col_prod, how='left')
                    df = df.dropna(subset=[trans_id_col, prod_name_col])
                    
                    basket = df.groupby(trans_id_col)[prod_name_col].apply(list).reset_index()
                    
                    basket = basket[basket[prod_name_col].apply(len) > 1]
                    
                    if not basket.empty:
                        product_pairs = []
                        for products in basket[prod_name_col]:
                            product_pairs.extend(combinations(products, 2))
                        
                        if product_pairs:
                            pair_counts = pd.Series(product_pairs).value_counts().reset_index()
                            pair_counts.columns = ['pair', 'count']
                            pair_counts['pair_str'] = pair_counts['pair'].apply(lambda x: f"{x[0]} + {x[1]}")
                            pair_counts = pair_counts.head(10)
                            
                            fig = px.bar(
                                pair_counts,
                                x='count',
                                y='pair_str',
                                orientation='h',
                                title='Top 10 Product Pairs Bought Together',
                                labels={'pair_str': 'Product Pair', 'count': 'Number of Transactions'},
                                color='count',
                                color_continuous_scale='ylorbr'
                            )
                            fig.update_layout(height=400)
                            st.plotly_chart(fig, use_container_width=True)
                            
                            top_pair = pair_counts.loc[pair_counts['count'].idxmax(), 'pair_str']
                            st.info(f"🤝 **Top Cross-Sell Pair:** {top_pair}")
                        else:
                            st.warning("No product pairs found in transactions.")
                    else:
                        st.warning("No transactions with multiple products found.")
                except Exception as e:
                    st.error(f"Error creating cross-selling analysis: {e}")
                    logger.error(f"Cross-selling error: {e}")
                    st.write("Sample of transaction data:", df[[trans_id_col, prod_name_col]].head())
            else:
                st.warning("Cross-selling data not available.")

    # Clean up connections
    if st.session_state.source_conn:
        try:
            st.session_state.source_conn.close()
        except:
            pass
        st.session_state.source_engine.dispose()