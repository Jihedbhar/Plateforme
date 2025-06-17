# pages/dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import os
import logging
from itertools import combinations

logger = logging.getLogger(__name__)

def dashboard_page():
    st.header("📊 B2C Retailer Performance Dashboard")
    st.markdown("**Unlock powerful insights to maximize sales and profits with data-driven decisions.**")

    # Check prerequisites
    if not check_dashboard_prerequisites():
        return

    # Show dashboard status
    show_dashboard_status()

    # Create main dashboard tabs
    create_dashboard_tabs()

def check_dashboard_prerequisites():
    """Check if all required data is available for dashboard"""
    
    # Check if ETL pipeline completed
    if not st.session_state.get('csv_paths'):
        st.error("⚠️ **Setup Required:** No exported data found.")
        st.info("👈 Please complete the **Setup** and **Transform & Export** steps first.")
        st.markdown("### Steps to get started:")
        st.markdown("1. **Setup**: Connect to your database and map tables")
        st.markdown("2. **Transform & Export**: Configure columns and export data")
        st.markdown("3. **Dashboard**: View analytics (you are here)")
        return False
    
    # Check for essential tables
    required_tables = ['Transactions']
    missing_tables = [table for table in required_tables if table not in st.session_state.csv_paths]
    
    if missing_tables:
        st.error(f"⚠️ **Missing Essential Data:** {', '.join(missing_tables)}")
        st.info("Please ensure Transactions table is exported in Transform & Export step.")
        return False
    
    return True

def show_dashboard_status():
    """Show current dashboard status and available data"""
    
    # Progress indicators
    available_tables = list(st.session_state.get('csv_paths', {}).keys())
    total_expected = 7  # Total French retail tables
    
    progress_col1, progress_col2, progress_col3 = st.columns(3)
    
    with progress_col1:
        st.metric("Tables Available", f"{len(available_tables)}/{total_expected}")
    
    with progress_col2:
        # Calculate total records across all tables
        total_records = 0
        for table_name, csv_path in st.session_state.get('csv_paths', {}).items():
            try:
                df = pd.read_csv(csv_path, nrows=0)  # Just get shape
                record_count = sum(1 for _ in open(csv_path)) - 1  # Count lines minus header
                total_records += record_count
            except:
                continue
        st.metric("Total Records", f"{total_records:,}")
    
    with progress_col3:
        db_name = "Unknown"
        if st.session_state.get('uploaded_filename'):
            db_name = st.session_state.uploaded_filename
        elif st.session_state.get('db_params', {}).get('database'):
            db_name = st.session_state.db_params['database']
        st.metric("Data Source", db_name)

    # Show available tables in sidebar
    with st.sidebar:
        st.markdown("### 📋 Available Data")
        
        expected_tables = ['Transactions', 'Client', 'Produit', 'Magasin', 'Stock', 'Employé', 'Localisation']
        for table in expected_tables:
            status = "✅" if table in available_tables else "❌"
            st.write(f"{status} {table}")
        
        if len(available_tables) < len(expected_tables):
            st.info(f"💡 {len(expected_tables) - len(available_tables)} more tables available in Transform & Export")

def create_dashboard_tabs():
    """Create the main dashboard tab interface"""
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Sales Analytics",
        "👥 Customer Insights", 
        "📦 Inventory Management",
        "🏪 Store Performance",
        "💰 Profitability Analysis"
    ])

    with tab1:
        create_sales_analytics()
    
    with tab2:
        create_customer_insights()
    
    with tab3:
        create_inventory_management()
    
    with tab4:
        create_store_performance()
    
    with tab5:
        create_profitability_analysis()

@st.cache_data
def load_table_data(table_name):
    """Safely load data from exported CSV files"""
    
    if table_name not in st.session_state.get('csv_paths', {}):
        return pd.DataFrame()
    
    try:
        csv_path = st.session_state.csv_paths[table_name]
        if not os.path.exists(csv_path):
            return pd.DataFrame()
        
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {table_name}: {len(df)} rows, {len(df.columns)} columns")
        return df
    
    except Exception as e:
        logger.error(f"Error loading {table_name}: {e}")
        return pd.DataFrame()

def get_column_mapping(table_name, column_name):
    """Get the actual column name from column mappings"""
    mappings = st.session_state.get('column_mappings', {}).get(table_name, {})
    return mappings.get(column_name, column_name)

def create_sales_analytics():
    """Create sales analytics visualizations"""
    
    st.subheader("📈 Sales Analytics")
    
    # Load data
    df_trans = load_table_data('Transactions')
    df_prod = load_table_data('Produit')
    
    if df_trans.empty:
        st.warning("⚠️ No transaction data available for sales analytics.")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        create_sales_trend_chart(df_trans)
    
    with col2:
        create_sales_by_category_chart(df_trans, df_prod)
    
    create_seasonal_analysis_chart(df_trans)

def create_sales_trend_chart(df_trans):
    """Create sales trend over time chart"""
    
    st.subheader("💰 Sales Trend Over Time")
    
    # Get column names
    date_col = get_column_mapping('Transactions', 'date_heure')
    amount_col = get_column_mapping('Transactions', 'montant_total')
    
    # Check required columns
    if date_col not in df_trans.columns or amount_col not in df_trans.columns:
        st.warning(f"Missing required columns: {date_col}, {amount_col}")
        return
    
    try:
        # Clean and process data
        df_clean = df_trans.copy()
        df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
        df_clean = df_clean.dropna(subset=[date_col, amount_col])
        
        if df_clean.empty:
            st.warning("No valid date/amount data found.")
            return
        
        # Group by month
        df_clean['month'] = df_clean[date_col].dt.to_period('M').astype(str)
        monthly_sales = df_clean.groupby('month')[amount_col].sum().reset_index()
        
        # Create chart
        fig = px.line(
            monthly_sales,
            x='month',
            y=amount_col,
            title='Monthly Sales Trend',
            labels={'month': 'Month', amount_col: 'Total Sales (TND)'}
        )
        fig.update_layout(xaxis_tickangle=45, height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Show metrics
        total_sales = df_clean[amount_col].sum()
        avg_monthly = monthly_sales[amount_col].mean()
        
        metric_col1, metric_col2 = st.columns(2)
        with metric_col1:
            st.metric("Total Sales", f"{total_sales:,.0f} TND")
        with metric_col2:
            st.metric("Avg Monthly", f"{avg_monthly:,.0f} TND")
            
    except Exception as e:
        st.error(f"Error creating sales trend: {e}")

def create_sales_by_category_chart(df_trans, df_prod):
    """Create sales by product category chart"""
    
    st.subheader("📊 Sales by Product Category")
    
    if df_trans.empty or df_prod.empty:
        st.warning("Transaction or product data not available.")
        return
    
    # Get column mappings
    trans_prod_col = get_column_mapping('Transactions', 'id_produit')
    amount_col = get_column_mapping('Transactions', 'montant_total')
    prod_id_col = get_column_mapping('Produit', 'id_produit')
    category_col = get_column_mapping('Produit', 'catégorie')
    
    required_cols_trans = [trans_prod_col, amount_col]
    required_cols_prod = [prod_id_col, category_col]
    
    if not all(col in df_trans.columns for col in required_cols_trans):
        st.warning(f"Missing transaction columns: {required_cols_trans}")
        return
    
    if not all(col in df_prod.columns for col in required_cols_prod):
        st.warning(f"Missing product columns: {required_cols_prod}")
        return
    
    try:
        # Merge data
        df_merged = df_trans.merge(
            df_prod[[prod_id_col, category_col]], 
            left_on=trans_prod_col, 
            right_on=prod_id_col, 
            how='left'
        )
        
        # Group by category
        category_sales = df_merged.groupby(category_col)[amount_col].sum().reset_index()
        category_sales = category_sales.sort_values(amount_col, ascending=True)
        
        # Create chart
        fig = px.bar(
            category_sales,
            x=amount_col,
            y=category_col,
            orientation='h',
            title='Sales by Product Category',
            labels={category_col: 'Category', amount_col: 'Total Sales (TND)'},
            color=amount_col,
            color_continuous_scale='viridis'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Show top category
        if not category_sales.empty:
            top_category = category_sales.loc[category_sales[amount_col].idxmax(), category_col]
            st.info(f"🥇 **Top Category:** {top_category}")
            
    except Exception as e:
        st.error(f"Error creating category analysis: {e}")

def create_seasonal_analysis_chart(df_trans):
    """Create seasonal sales pattern analysis"""
    
    st.subheader("🌞 Seasonal Sales Patterns")
    
    date_col = get_column_mapping('Transactions', 'date_heure')
    amount_col = get_column_mapping('Transactions', 'montant_total')
    
    if date_col not in df_trans.columns or amount_col not in df_trans.columns:
        st.warning("Date or amount column not available for seasonal analysis.")
        return
    
    try:
        # Process data
        df_clean = df_trans.copy()
        df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
        df_clean = df_clean.dropna(subset=[date_col, amount_col])
        
        if df_clean.empty:
            st.warning("No valid data for seasonal analysis.")
            return
        
        # Group by month
        df_clean['month_num'] = df_clean[date_col].dt.month
        seasonal_sales = df_clean.groupby('month_num')[amount_col].mean().reset_index()
        
        # Add month names
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        seasonal_sales['month_name'] = seasonal_sales['month_num'].map(
            lambda x: month_names[int(x)-1] if 1 <= x <= 12 else 'Unknown'
        )
        
        # Create chart
        fig = px.bar(
            seasonal_sales,
            x='month_name',
            y=amount_col,
            title='Average Sales by Month (Seasonality)',
            labels={'month_name': 'Month', amount_col: 'Average Sales (TND)'},
            color=amount_col,
            color_continuous_scale='blues'
        )
        fig.update_layout(height=400, xaxis={'categoryorder': 'array', 'categoryarray': month_names})
        st.plotly_chart(fig, use_container_width=True)
        
        # Show peak season
        if not seasonal_sales.empty:
            peak_month = seasonal_sales.loc[seasonal_sales[amount_col].idxmax(), 'month_name']
            st.info(f"🌟 **Peak Sales Season:** {peak_month}")
            
    except Exception as e:
        st.error(f"Error creating seasonal analysis: {e}")

def create_customer_insights():
    """Create customer insights visualizations"""
    
    st.subheader("👥 Customer Insights")
    
    df_client = load_table_data('Client')
    df_trans = load_table_data('Transactions')
    
    if df_client.empty and df_trans.empty:
        st.warning("⚠️ No customer or transaction data available.")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        create_customer_loyalty_chart(df_client, df_trans)
    
    with col2:
        create_customer_geography_chart(df_client)
    
    create_customer_segmentation_chart(df_trans)

def create_customer_loyalty_chart(df_client, df_trans):
    """Create customer loyalty distribution chart"""
    
    st.subheader("🎖️ Customer Loyalty Distribution")
    
    if df_client.empty:
        st.warning("Customer data not available.")
        return
    
    loyalty_col = get_column_mapping('Client', 'Tier_fidelité')
    
    if loyalty_col not in df_client.columns:
        st.warning(f"Loyalty column '{loyalty_col}' not found.")
        return
    
    try:
        # Simple loyalty distribution
        loyalty_dist = df_client[loyalty_col].value_counts().reset_index()
        loyalty_dist.columns = [loyalty_col, 'count']
        
        fig = px.pie(
            loyalty_dist,
            names=loyalty_col,
            values='count',
            title='Customer Distribution by Loyalty Tier',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        total_customers = len(df_client)
        st.metric("Total Customers", f"{total_customers:,}")
        
    except Exception as e:
        st.error(f"Error creating loyalty chart: {e}")

def create_customer_geography_chart(df_client):
    """Create customer geographic distribution"""
    
    st.subheader("📍 Customer Geographic Distribution")
    
    if df_client.empty:
        st.warning("Customer data not available.")
        return
    
    city_col = get_column_mapping('Client', 'ville')
    
    if city_col not in df_client.columns:
        st.warning(f"City column '{city_col}' not found.")
        return
    
    try:
        city_dist = df_client[city_col].value_counts().head(10).reset_index()
        city_dist.columns = [city_col, 'count']
        
        fig = px.bar(
            city_dist,
            x='count',
            y=city_col,
            orientation='h',
            title='Top 10 Cities by Customer Count',
            labels={city_col: 'City', 'count': 'Customers'},
            color='count',
            color_continuous_scale='blues'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error creating geography chart: {e}")

def create_customer_segmentation_chart(df_trans):
    """Create customer spending segmentation"""
    
    st.subheader("💸 Customer Spending Segmentation")
    
    if df_trans.empty:
        st.warning("Transaction data not available.")
        return
    
    client_col = get_column_mapping('Transactions', 'id_client')
    amount_col = get_column_mapping('Transactions', 'montant_total')
    
    if client_col not in df_trans.columns or amount_col not in df_trans.columns:
        st.warning("Required columns not found for segmentation.")
        return
    
    try:
        # Calculate customer spending
        customer_spending = df_trans.groupby(client_col)[amount_col].sum().reset_index()
        
        # Create segments
        bins = [0, 500, 2000, float('inf')]
        labels = ['Low Spender', 'Medium Spender', 'High Spender']
        customer_spending['segment'] = pd.cut(
            customer_spending[amount_col],
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
            labels={'segment': 'Spending Segment', 'count': 'Customers'},
            color='count',
            color_continuous_scale='purples'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Show high spenders metric
        high_spenders = segment_counts[segment_counts['segment'] == 'High Spender']['count'].iloc[0] if not segment_counts.empty else 0
        st.metric("High Spenders", high_spenders)
        
    except Exception as e:
        st.error(f"Error creating segmentation: {e}")

def create_inventory_management():
    """Create inventory management visualizations"""
    
    st.subheader("📦 Inventory Management")
    
    df_stock = load_table_data('Stock')
    df_prod = load_table_data('Produit')
    df_trans = load_table_data('Transactions')
    
    col1, col2 = st.columns(2)
    
    with col1:
        create_low_stock_alert(df_stock, df_prod)
    
    with col2:
        create_top_products_chart(df_trans, df_prod)
    
    create_sales_velocity_chart(df_trans, df_prod)

def create_low_stock_alert(df_stock, df_prod):
    """Create low stock alert visualization"""
    
    st.subheader("⚠️ Low Stock Alert")
    
    if df_stock.empty or df_prod.empty:
        st.warning("Stock or product data not available.")
        return
    
    # Get column mappings
    stock_prod_col = get_column_mapping('Stock', 'id_produit')
    quantity_col = get_column_mapping('Stock', 'quantité')
    threshold_col = get_column_mapping('Stock', 'seuil_minimum')
    prod_id_col = get_column_mapping('Produit', 'id_produit')
    prod_name_col = get_column_mapping('Produit', 'nom_produit')
    
    required_stock_cols = [stock_prod_col, quantity_col, threshold_col]
    required_prod_cols = [prod_id_col, prod_name_col]
    
    if not all(col in df_stock.columns for col in required_stock_cols):
        st.warning("Required stock columns not available.")
        return
    
    if not all(col in df_prod.columns for col in required_prod_cols):
        st.warning("Required product columns not available.")
        return
    
    try:
        # Merge stock and product data
        df_merged = df_stock.merge(
            df_prod[[prod_id_col, prod_name_col]], 
            left_on=stock_prod_col, 
            right_on=prod_id_col, 
            how='left'
        )
        
        # Find low stock items
        df_low = df_merged[df_merged[quantity_col] < df_merged[threshold_col]].head(10)
        
        if df_low.empty:
            st.success("✅ All products are above minimum stock threshold!")
        else:
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
            
    except Exception as e:
        st.error(f"Error creating stock alert: {e}")

def create_top_products_chart(df_trans, df_prod):
    """Create top selling products chart"""
    
    st.subheader("💎 Top Selling Products")
    
    if df_trans.empty or df_prod.empty:
        st.warning("Transaction or product data not available.")
        return
    
    # Get column mappings
    trans_prod_col = get_column_mapping('Transactions', 'id_produit')
    quantity_col = get_column_mapping('Transactions', 'quantité')
    prod_id_col = get_column_mapping('Produit', 'id_produit')
    prod_name_col = get_column_mapping('Produit', 'nom_produit')
    
    if (trans_prod_col not in df_trans.columns or quantity_col not in df_trans.columns or
        prod_id_col not in df_prod.columns or prod_name_col not in df_prod.columns):
        st.warning("Required columns not available for top products analysis.")
        return
    
    try:
        # Merge and aggregate
        df_merged = df_trans.merge(
            df_prod[[prod_id_col, prod_name_col]], 
            left_on=trans_prod_col, 
            right_on=prod_id_col, 
            how='left'
        )
        
        top_products = df_merged.groupby(prod_name_col)[quantity_col].sum().reset_index()
        top_products = top_products.sort_values(quantity_col, ascending=True).tail(10)
        
        fig = px.bar(
            top_products,
            x=quantity_col,
            y=prod_name_col,
            orientation='h',
            title='Top 10 Best Selling Products',
            labels={prod_name_col: 'Product', quantity_col: 'Total Quantity Sold'},
            color=quantity_col,
            color_continuous_scale='greens'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error creating top products chart: {e}")

def create_sales_velocity_chart(df_trans, df_prod):
    """Create sales velocity analysis"""
    
    st.subheader("🚀 Product Sales Velocity")
    
    if df_trans.empty or df_prod.empty:
        st.warning("Transaction or product data not available.")
        return
    
    # Get column mappings
    trans_prod_col = get_column_mapping('Transactions', 'id_produit')
    quantity_col = get_column_mapping('Transactions', 'quantité')
    date_col = get_column_mapping('Transactions', 'date_heure')
    prod_id_col = get_column_mapping('Produit', 'id_produit')
    prod_name_col = get_column_mapping('Produit', 'nom_produit')
    
    required_cols = [trans_prod_col, quantity_col, date_col, prod_id_col, prod_name_col]
    
    if (not all(col in df_trans.columns for col in [trans_prod_col, quantity_col, date_col]) or
        not all(col in df_prod.columns for col in [prod_id_col, prod_name_col])):
        st.warning("Required columns not available for velocity analysis.")
        return
    
    try:
        # Process dates
        df_trans_clean = df_trans.copy()
        df_trans_clean[date_col] = pd.to_datetime(df_trans_clean[date_col], errors='coerce')
        df_trans_clean = df_trans_clean.dropna(subset=[date_col, quantity_col])
        
        if df_trans_clean.empty:
            st.warning("No valid data for velocity analysis.")
            return
        
        # Get recent data (last 30 days)
        recent_date = df_trans_clean[date_col].max()
        df_recent = df_trans_clean[df_trans_clean[date_col] >= recent_date - timedelta(days=30)]
        
        # Merge with products
        df_merged = df_recent.merge(
            df_prod[[prod_id_col, prod_name_col]], 
            left_on=trans_prod_col, 
            right_on=prod_id_col, 
            how='left'
        )
        
        # Calculate velocity
        velocity = df_merged.groupby(prod_name_col)[quantity_col].sum().reset_index()
        velocity['velocity'] = velocity[quantity_col] / 30  # Daily average
        velocity = velocity.sort_values('velocity', ascending=True).tail(10)
        
        fig = px.bar(
            velocity,
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
        
        if not velocity.empty:
            fastest = velocity.loc[velocity['velocity'].idxmax(), prod_name_col]
            st.info(f"⚡ **Fastest Moving Product:** {fastest}")
            
    except Exception as e:
        st.error(f"Error creating velocity analysis: {e}")

def create_store_performance():
    """Create store performance visualizations"""
    
    st.subheader("🏪 Store Performance")
    
    df_trans = load_table_data('Transactions')
    df_magasin = load_table_data('Magasin')
    df_location = load_table_data('Localisation')
    df_employee = load_table_data('Employé')
    
    col1, col2 = st.columns(2)
    
    with col1:
        create_geographic_sales_chart(df_trans, df_magasin, df_location)
    
    with col2:
        create_employee_performance_chart(df_trans, df_employee)

def create_geographic_sales_chart(df_trans, df_magasin, df_location):
    """Create sales by geographic location chart"""
    
    st.subheader("🌍 Sales by Geographic Location")
    
    if df_trans.empty or df_magasin.empty or df_location.empty:
        st.warning("Required data for geographic analysis not available.")
        return
    
    # Get column mappings
    trans_store_col = get_column_mapping('Transactions', 'id_magasin')
    amount_col = get_column_mapping('Transactions', 'montant_total')
    store_id_col = get_column_mapping('Magasin', 'id_magasin')
    store_loc_col = get_column_mapping('Magasin', 'id_localisation')
    loc_id_col = get_column_mapping('Localisation', 'id_localisation')
    city_col = get_column_mapping('Localisation', 'ville')
    
    try:
        # Merge all data
        df_merged = df_trans.merge(
            df_magasin[[store_id_col, store_loc_col]], 
            left_on=trans_store_col, 
            right_on=store_id_col, 
            how='left'
        )
        df_merged = df_merged.merge(
            df_location[[loc_id_col, city_col]], 
            left_on=store_loc_col, 
            right_on=loc_id_col, 
            how='left'
        )
        
        # Group by city
        city_sales = df_merged.groupby(city_col)[amount_col].sum().reset_index()
        city_sales = city_sales.sort_values(amount_col, ascending=True)
        
        fig = px.bar(
            city_sales,
            x=amount_col,
            y=city_col,
            orientation='h',
            title='Sales Performance by City',
            labels={city_col: 'City', amount_col: 'Total Sales (TND)'},
            color=amount_col,
            color_continuous_scale='plasma'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        if not city_sales.empty:
            top_city = city_sales.loc[city_sales[amount_col].idxmax(), city_col]
            st.info(f"🏆 **Top Performing City:** {top_city}")
            
    except Exception as e:
        st.error(f"Error creating geographic sales chart: {e}")

def create_employee_performance_chart(df_trans, df_employee):
    """Create employee performance chart"""
    
    st.subheader("👷 Employee Sales Performance")
    
    if df_trans.empty or df_employee.empty:
        st.warning("Transaction or employee data not available.")
        return
    
    # Get column mappings
    trans_emp_col = get_column_mapping('Transactions', 'id_employé')
    amount_col = get_column_mapping('Transactions', 'montant_total')
    emp_id_col = get_column_mapping('Employé', 'id_employé')
    position_col = get_column_mapping('Employé', 'poste')
    
    if (trans_emp_col not in df_trans.columns or amount_col not in df_trans.columns or
        emp_id_col not in df_employee.columns or position_col not in df_employee.columns):
        st.warning("Required columns not available for employee analysis.")
        return
    
    try:
        # Merge data
        df_merged = df_trans.merge(
            df_employee[[emp_id_col, position_col]], 
            left_on=trans_emp_col, 
            right_on=emp_id_col, 
            how='left'
        )
        
        # Group by employee
        emp_sales = df_merged.groupby(trans_emp_col)[amount_col].sum().reset_index()
        emp_sales = emp_sales.sort_values(amount_col, ascending=True).tail(10)
        
        fig = px.bar(
            emp_sales,
            x=amount_col,
            y=trans_emp_col,
            orientation='h',
            title='Top 10 Employees by Sales',
            labels={trans_emp_col: 'Employee ID', amount_col: 'Total Sales (TND)'},
            color=amount_col,
            color_continuous_scale='teal'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        if not emp_sales.empty:
            top_employee = emp_sales.loc[emp_sales[amount_col].idxmax(), trans_emp_col]
            st.info(f"🌟 **Top Performing Employee:** ID {top_employee}")
            
    except Exception as e:
        st.error(f"Error creating employee performance chart: {e}")

def create_profitability_analysis():
    """Create profitability analysis visualizations"""
    
    st.subheader("💰 Profitability Analysis")
    
    df_trans = load_table_data('Transactions')
    df_prod = load_table_data('Produit')
    
    col1, col2 = st.columns(2)
    
    with col1:
        create_profit_margin_chart(df_trans, df_prod)
    
    with col2:
        create_cross_selling_chart(df_trans, df_prod)
    
    create_profitability_summary(df_trans, df_prod)

def create_profit_margin_chart(df_trans, df_prod):
    """Create product profit margins chart"""
    
    st.subheader("💎 Product Profit Margins")
    
    if df_trans.empty or df_prod.empty:
        st.warning("Transaction or product data not available.")
        return
    
    # Get column mappings
    trans_prod_col = get_column_mapping('Transactions', 'id_produit')
    quantity_col = get_column_mapping('Transactions', 'quantité')
    prod_id_col = get_column_mapping('Produit', 'id_produit')
    prod_name_col = get_column_mapping('Produit', 'nom_produit')
    sale_price_col = get_column_mapping('Produit', 'prix_vente')
    purchase_price_col = get_column_mapping('Produit', 'prix_achat')
    
    required_trans_cols = [trans_prod_col, quantity_col]
    required_prod_cols = [prod_id_col, prod_name_col, sale_price_col, purchase_price_col]
    
    if (not all(col in df_trans.columns for col in required_trans_cols) or
        not all(col in df_prod.columns for col in required_prod_cols)):
        st.warning("Required columns not available for profit analysis.")
        return
    
    try:
        # Merge data
        df_merged = df_trans.merge(
            df_prod[[prod_id_col, prod_name_col, sale_price_col, purchase_price_col]], 
            left_on=trans_prod_col, 
            right_on=prod_id_col, 
            how='left'
        )
        
        # Clean data
        df_clean = df_merged.dropna(subset=[sale_price_col, purchase_price_col, quantity_col])
        
        if df_clean.empty:
            st.warning("No valid data for profit margin analysis.")
            return
        
        # Calculate profits
        df_clean['unit_profit'] = df_clean[sale_price_col] - df_clean[purchase_price_col]
        df_clean['total_profit'] = df_clean['unit_profit'] * df_clean[quantity_col]
        df_clean['total_revenue'] = df_clean[sale_price_col] * df_clean[quantity_col]
        
        # Group by product
        profit_by_product = df_clean.groupby(prod_name_col).agg({
            'total_profit': 'sum',
            'total_revenue': 'sum'
        }).reset_index()
        
        # Calculate margin
        profit_by_product['margin'] = profit_by_product['total_profit'] / profit_by_product['total_revenue']
        
        # Filter valid margins
        profit_by_product = profit_by_product[
            (profit_by_product['total_revenue'] > 0) &
            (profit_by_product['margin'].notna()) &
            (profit_by_product['margin'] != np.inf) &
            (profit_by_product['margin'] != -np.inf)
        ]
        
        if profit_by_product.empty:
            st.warning("No valid profit margins calculated.")
            return
        
        # Top 10 by margin
        top_margins = profit_by_product.sort_values('margin', ascending=False).head(10)
        
        fig = px.bar(
            top_margins,
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
        
        if not top_margins.empty:
            best_margin_product = top_margins.iloc[0][prod_name_col]
            best_margin = top_margins.iloc[0]['margin']
            st.info(f"💎 **Highest Margin Product:** {best_margin_product} ({best_margin:.1%})")
            
    except Exception as e:
        st.error(f"Error creating profit margin analysis: {e}")

def create_cross_selling_chart(df_trans, df_prod):
    """Create cross-selling opportunities chart"""
    
    st.subheader("🤝 Cross-Selling Opportunities")
    
    if df_trans.empty or df_prod.empty:
        st.warning("Transaction or product data not available.")
        return
    
    # Get column mappings
    trans_id_col = get_column_mapping('Transactions', 'id_transaction')
    trans_prod_col = get_column_mapping('Transactions', 'id_produit')
    prod_id_col = get_column_mapping('Produit', 'id_produit')
    prod_name_col = get_column_mapping('Produit', 'nom_produit')
    
    if (trans_id_col not in df_trans.columns or trans_prod_col not in df_trans.columns or
        prod_id_col not in df_prod.columns or prod_name_col not in df_prod.columns):
        st.warning("Required columns not available for cross-selling analysis.")
        return
    
    try:
        # Merge data
        df_merged = df_trans.merge(
            df_prod[[prod_id_col, prod_name_col]], 
            left_on=trans_prod_col, 
            right_on=prod_id_col, 
            how='left'
        )
        
        # Clean data
        df_clean = df_merged.dropna(subset=[trans_id_col, prod_name_col])
        
        if df_clean.empty:
            st.warning("No valid data for cross-selling analysis.")
            return
        
        # Group products by transaction
        baskets = df_clean.groupby(trans_id_col)[prod_name_col].apply(list).reset_index()
        
        # Filter transactions with multiple products
        multi_product_baskets = baskets[baskets[prod_name_col].apply(len) > 1]
        
        if multi_product_baskets.empty:
            st.warning("No transactions with multiple products found.")
            return
        
        # Find product pairs
        product_pairs = []
        for products in multi_product_baskets[prod_name_col]:
            if len(products) >= 2:
                product_pairs.extend(combinations(products, 2))
        
        if not product_pairs:
            st.warning("No product pairs found.")
            return
        
        # Count pair occurrences
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
            labels={'pair_str': 'Product Pair', 'count': 'Co-occurrence Count'},
            color='count',
            color_continuous_scale='viridis'
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        if not pair_counts.empty:
            top_pair = pair_counts.iloc[0]['pair_str']
            top_count = pair_counts.iloc[0]['count']
            st.info(f"🤝 **Top Cross-Sell Pair:** {top_pair} ({top_count} times)")
            
    except Exception as e:
        st.error(f"Error creating cross-selling analysis: {e}")

def create_profitability_summary(df_trans, df_prod):
    """Create overall profitability summary"""
    
    st.subheader("📊 Profitability Summary")
    
    if df_trans.empty or df_prod.empty:
        st.warning("Data not available for profitability summary.")
        return
    
    try:
        # Get column mappings
        trans_prod_col = get_column_mapping('Transactions', 'id_produit')
        quantity_col = get_column_mapping('Transactions', 'quantité')
        amount_col = get_column_mapping('Transactions', 'montant_total')
        prod_id_col = get_column_mapping('Produit', 'id_produit')
        sale_price_col = get_column_mapping('Produit', 'prix_vente')
        purchase_price_col = get_column_mapping('Produit', 'prix_achat')
        
        # Check required columns
        required_trans_cols = [trans_prod_col, quantity_col, amount_col]
        required_prod_cols = [prod_id_col, sale_price_col, purchase_price_col]
        
        if (not all(col in df_trans.columns for col in required_trans_cols) or
            not all(col in df_prod.columns for col in required_prod_cols)):
            st.warning("Required columns not available for profitability summary.")
            return
        
        # Merge data
        df_merged = df_trans.merge(
            df_prod[[prod_id_col, sale_price_col, purchase_price_col]], 
            left_on=trans_prod_col, 
            right_on=prod_id_col, 
            how='left'
        )
        
        # Clean data
        df_clean = df_merged.dropna(subset=[quantity_col, sale_price_col, purchase_price_col])
        
        if df_clean.empty:
            st.warning("No valid data for profitability calculations.")
            return
        
        # Calculate metrics
        total_revenue = df_clean[amount_col].sum()
        total_cost = (df_clean[purchase_price_col] * df_clean[quantity_col]).sum()
        total_profit = total_revenue - total_cost
        overall_margin = total_profit / total_revenue if total_revenue > 0 else 0
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Revenue", f"{total_revenue:,.0f} TND")
        
        with col2:
            st.metric("Total Cost", f"{total_cost:,.0f} TND")
        
        with col3:
            st.metric("Total Profit", f"{total_profit:,.0f} TND")
        
        with col4:
            st.metric("Overall Margin", f"{overall_margin:.1%}")
        
        # Profit trend over time
        date_col = get_column_mapping('Transactions', 'date_heure')
        if date_col in df_clean.columns:
            df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
            df_clean = df_clean.dropna(subset=[date_col])
            
            if not df_clean.empty:
                df_clean['profit'] = (df_clean[sale_price_col] - df_clean[purchase_price_col]) * df_clean[quantity_col]
                df_clean['month'] = df_clean[date_col].dt.to_period('M').astype(str)
                
                monthly_profit = df_clean.groupby('month')['profit'].sum().reset_index()
                
                fig = px.line(
                    monthly_profit,
                    x='month',
                    y='profit',
                    title='Monthly Profit Trend',
                    labels={'month': 'Month', 'profit': 'Profit (TND)'}
                )
                fig.update_layout(xaxis_tickangle=45, height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        # Performance insights
        if total_profit > 0:
            st.success(f"✅ **Profitable Business** - Total profit of {total_profit:,.0f} TND")
        else:
            st.error(f"❌ **Loss-Making** - Total loss of {abs(total_profit):,.0f} TND")
        
        if overall_margin > 0.2:
            st.info("💡 **Healthy Margins** - Consider expanding high-margin products")
        elif overall_margin > 0.1:
            st.warning("⚠️ **Moderate Margins** - Look for cost optimization opportunities")
        else:
            st.error("🚨 **Low Margins** - Urgent review of pricing and costs needed")
            
    except Exception as e:
        st.error(f"Error creating profitability summary: {e}")

# Run dashboard
if __name__ == "__main__":
    dashboard_page()