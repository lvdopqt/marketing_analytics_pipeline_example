import streamlit as st
import pandas as pd
import sqlite3
import os
import plotly.express as px
import logging
import sys # Import the sys module

# Add the project root directory to the Python path
# This allows importing modules from the 'src' directory
# Get the directory of the current script (__file__)
script_dir = os.path.dirname(os.path.abspath(__file__))
# Go up two levels to the project root directory
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
# Add the project root to sys.path
sys.path.insert(0, project_root)


# Import the summarize_metrics function from transformation
# This import should now work because the project root is in sys.path
from src.transformation.metric_calculation import summarize_metrics

# Set up basic logging (optional for Streamlit app)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
# Define paths relative to where the Streamlit app will be run from (project root)
# Using environment variables or a config file is best practice for paths
DATA_SOURCE_PATH = os.environ.get('ANALYTICS_DATA_PATH', 'data/analytics.db') # Default to SQLite
DATA_SOURCE_FORMAT = os.environ.get('ANALYTICS_DATA_FORMAT', 'sqlite') # 'sqlite' or 'parquet'
SQLITE_TABLE_NAME = os.environ.get('ANALYTICS_SQLITE_TABLE', 'marketing_analytics')

# --- Data Loading Function ---
@st.cache_data # Cache data loading to improve performance
def load_analytics_data(data_path: str, data_format: str, table_name: str | None = None) -> pd.DataFrame | None:
    """
    Loads the final analytics data from the specified source.

    Args:
        data_path: Path to the data source (SQLite DB file or Parquet file).
        data_format: Format of the data source ('sqlite' or 'parquet').
        table_name: Table name if data_format is 'sqlite'.

    Returns:
        A pandas DataFrame containing the analytics data, or None if loading fails.
    """
    logging.info(f"Attempting to load data from {data_path} (Format: {data_format})")
    df = None
    try:
        if data_format == 'sqlite':
            if not os.path.exists(data_path):
                 logging.error(f"SQLite DB file not found at {data_path}")
                 return None
            conn = sqlite3.connect(data_path)
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            conn.close()
            logging.info(f"Successfully loaded {len(df)} records from SQLite.")
        elif data_format == 'parquet':
            if not os.path.exists(data_path):
                 logging.error(f"Parquet file not found at {data_path}")
                 return None
            df = pd.read_parquet(data_path)
            logging.info(f"Successfully loaded {len(df)} records from Parquet.")
        else:
            logging.error(f"Unsupported data format specified: {data_format}")
            return None

        # Ensure date column is datetime for plotting/filtering
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            # Drop rows where date could not be parsed if necessary
            df.dropna(subset=['date'], inplace=True)


        return df

    except Exception as e:
        logging.error(f"An error occurred during data loading for dashboard: {e}")
        return None

# --- Filtering Logic ---
def apply_filters(df: pd.DataFrame, selected_client: str, selected_platform: str, selected_date_range: tuple) -> pd.DataFrame:
    """Applies filters based on user selections."""
    filtered_df = df.copy()

    if selected_client != 'All':
        filtered_df = filtered_df[filtered_df['client_id'] == selected_client]

    if selected_platform != 'All':
        filtered_df = filtered_df[filtered_df['platform'] == selected_platform]

    if len(selected_date_range) == 2:
        start_date = pd.to_datetime(selected_date_range[0])
        end_date = pd.to_datetime(selected_date_range[1])
        filtered_df = filtered_df[(filtered_df['date'] >= start_date) & (filtered_df['date'] <= end_date)]
    elif len(selected_date_range) == 1:
         single_date = pd.to_datetime(selected_date_range[0])
         filtered_df = filtered_df[filtered_df['date'] == single_date]

    return filtered_df

# --- Display Functions ---
def display_key_metrics(df: pd.DataFrame):
    """Displays key aggregated metrics using Streamlit columns."""
    st.subheader("Key Metrics")

    total_spend = df['spend_usd'].sum() if 'spend_usd' in df.columns else 0
    total_clicks = df['clicks'].sum() if 'clicks' in df.columns else 0
    total_impressions = df['impressions'].sum() if 'impressions' in df.columns else 0
    total_sessions = df['sessions'].sum() if 'sessions' in df.columns else 0
    total_emails_sent = df['emails_sent'].sum() if 'emails_sent' in df.columns else 0
    total_attributed_revenue = df['attributed_revenue_from_source_usd'].sum() if 'attributed_revenue_from_source_usd' in df.columns else 0

    col1, col2, col3, col4, col5, col6 = st.columns(6)

    col1.metric("Total Spend (USD)", f"${total_spend:,.2f}")
    col2.metric("Total Clicks", f"{total_clicks:,}")
    col3.metric("Total Impressions", f"{total_impressions:,}")
    col4.metric("Total Sessions", f"{total_sessions:,}")
    col5.metric("Total Emails Sent", f"{total_emails_sent:,}")
    col6.metric("Total Attributed Revenue (USD)", f"${total_attributed_revenue:,.2f}")

def display_visualizations(df: pd.DataFrame):
    """Displays various marketing data visualizations."""
    st.subheader("Visualizations")

    # Example 1: Spend by Platform
    if 'platform' in df.columns and 'spend_usd' in df.columns:
        spend_by_platform = df.groupby('platform')['spend_usd'].sum().reset_index()
        fig_spend_platform = px.bar(
            spend_by_platform, x='platform', y='spend_usd', title='Total Spend by Platform'
        )
        st.plotly_chart(fig_spend_platform, use_container_width=True)
    else:
         st.warning("Cannot generate Spend by Platform chart: Missing 'platform' or 'spend_usd' column.")

    # Example 2: Clicks Over Time
    if 'date' in df.columns and 'clicks' in df.columns:
         daily_clicks = df.groupby('date')['clicks'].sum().reset_index()
         fig_clicks_time = px.line(
             daily_clicks, x='date', y='clicks', title='Total Clicks Over Time'
         )
         st.plotly_chart(fig_clicks_time, use_container_width=True)
    else:
         st.warning("Cannot generate Clicks Over Time chart: Missing 'date' or 'clicks' column.")

    # Example 3: CTR by Platform
    if 'platform' in df.columns and 'clicks' in df.columns and 'impressions' in df.columns:
         ctr_platforms_df = df[df['platform'].isin(['google_ads', 'facebook_ads'])].copy()
         if not ctr_platforms_df.empty:
              agg_ctr_by_platform = ctr_platforms_df.groupby('platform').agg(
                  total_clicks=('clicks', 'sum'),
                  total_impressions=('impressions', 'sum')
              ).reset_index()
              agg_ctr_by_platform['aggregate_ctr'] = 0.0
              valid_impressions_mask = agg_ctr_by_platform['total_impressions'] > 0
              agg_ctr_by_platform.loc[valid_impressions_mask, 'aggregate_ctr'] = (
                   (agg_ctr_by_platform.loc[valid_impressions_mask, 'total_clicks'] / agg_ctr_by_platform.loc[valid_impressions_mask, 'total_impressions']) * 100
              )
              fig_ctr_platform = px.bar(
                  agg_ctr_by_platform, x='platform', y='aggregate_ctr', title='Aggregate CTR by Platform (%)'
              )
              st.plotly_chart(fig_ctr_platform, use_container_width=True)
         else:
              st.warning("No Google Ads or Facebook Ads data in filtered selection to calculate CTR by Platform.")
    else:
         st.warning("Cannot generate CTR by Platform chart: Missing required columns.")

    # Example 4: Sessions vs Pageviews by Date
    if 'date' in df.columns and 'sessions' in df.columns and 'pageviews' in df.columns:
         daily_web_metrics = df[df['platform'] == 'web_traffic'].groupby('date').agg(
             total_sessions=('sessions', 'sum'),
             total_pageviews=('pageviews', 'sum')
         ).reset_index()
         if not daily_web_metrics.empty:
              fig_web_metrics = px.line(
                  daily_web_metrics, x='date', y=['total_sessions', 'total_pageviews'], title='Sessions vs Pageviews Over Time (Web Traffic)'
              )
              st.plotly_chart(fig_web_metrics, use_container_width=True)
         else:
              st.warning("No Web Traffic data in filtered selection to show Sessions vs Pageviews.")
    else:
         st.warning("Cannot generate Sessions vs Pageviews chart: Missing required columns.")

    # Example 5: Attributed Revenue by Channel Over Time
    if 'date' in df.columns and 'platform' in df.columns and 'attributed_revenue_from_source_usd' in df.columns:
         daily_attributed_revenue = df.groupby(['date', 'platform'])['attributed_revenue_from_source_usd'].sum().reset_index()
         daily_attributed_revenue = daily_attributed_revenue[daily_attributed_revenue['attributed_revenue_from_source_usd'] > 0] # Filter out zero revenue rows

         if not daily_attributed_revenue.empty:
              fig_revenue_time = px.line(
                  daily_attributed_revenue, x='date', y='attributed_revenue_from_source_usd', color='platform', title='Attributed Revenue by Channel Over Time (USD)'
              )
              st.plotly_chart(fig_revenue_time, use_container_width=True)
         else:
              st.warning("No attributed revenue data in filtered selection to show trends.")
    else:
         st.warning("Cannot generate Attributed Revenue trend chart: Missing required columns.")


def display_report_charts(df: pd.DataFrame):
    """Displays charts for the summary reports."""
    st.subheader("Summary Reports (Charts)")

    # Report 1: Total Clicks by Platform (Chart)
    if 'clicks' in df.columns and 'platform' in df.columns:
        st.write("#### Total Clicks by Platform")
        total_clicks_by_platform = df.groupby('platform')['clicks'].sum().reset_index()
        if not total_clicks_by_platform.empty:
             fig_clicks_platform = px.bar(
                 total_clicks_by_platform, x='platform', y='clicks', title='Total Clicks by Platform'
             )
             st.plotly_chart(fig_clicks_platform, use_container_width=True)
        else:
             st.warning("No data to generate Total Clicks by Platform chart with current filters.")
    else:
         st.warning("Cannot generate Total Clicks by Platform chart: Missing required columns.")

    # Report 2: CTR Trends Over Time (Chart)
    if 'clicks' in df.columns and 'impressions' in df.columns and 'date' in df.columns:
         ad_platforms_df = df[df['platform'].isin(['google_ads', 'facebook_ads'])].copy()
         if not ad_platforms_df.empty:
              st.write("#### CTR Trends Over Time")
              daily_ad_performance = ad_platforms_df.groupby('date').agg(
                  total_clicks=('clicks', 'sum'),
                  total_impressions=('impressions', 'sum')
              ).reset_index()
              daily_ad_performance['daily_ctr'] = 0.0
              valid_impressions_mask = daily_ad_performance['total_impressions'] > 0
              daily_ad_performance.loc[valid_impressions_mask, 'daily_ctr'] = (
                  (daily_ad_performance.loc[valid_impressions_mask, 'total_clicks'] / daily_ad_performance.loc[valid_impressions_mask, 'total_impressions']) * 100
              )
              if not daily_ad_performance.empty:
                   fig_ctr_trends = px.line(
                       daily_ad_performance, x='date', y='daily_ctr', title='Aggregate CTR Trends Over Time (%)'
                   )
                   st.plotly_chart(fig_ctr_trends, use_container_width=True)
              else:
                   st.warning("No data to generate CTR Trends chart with current filters.")
         else:
              st.warning("No data from Google Ads or Facebook Ads in filtered selection to calculate CTR trends.")
    else:
         st.warning("Cannot generate CTR Trends chart: Missing required columns.")

# --- Main Streamlit App Logic ---
def main():
    st.set_page_config(layout="wide")
    st.title("Marketing Analytics Dashboard")

    # Load data
    analytics_df = load_analytics_data(DATA_SOURCE_PATH, DATA_SOURCE_FORMAT, SQLITE_TABLE_NAME)

    if analytics_df is None:
        st.error("Failed to load analytics data. Please ensure the data pipeline has run successfully and the data file/database exists.")
        return # Stop execution if data loading fails
    elif analytics_df.empty:
        st.warning("Analytics data loaded successfully, but the dataset is empty.")
        # Still display layout but with warnings in chart sections
        filtered_df = pd.DataFrame(columns=analytics_df.columns) # Pass empty df with columns
    else:
        st.success(f"Data loaded successfully. Showing data from {analytics_df['date'].min().strftime('%Y-%m-%d')} to {analytics_df['date'].max().strftime('%Y-%m-%d')}")
        # --- Filters ---
        st.sidebar.header("Filters")

        all_clients = ['All'] + sorted(analytics_df['client_id'].unique().tolist())
        selected_client = st.sidebar.selectbox("Select Client", all_clients)

        all_platforms = ['All'] + sorted(analytics_df['platform'].unique().tolist())
        selected_platform = st.sidebar.selectbox("Select Platform", all_platforms)

        min_date = analytics_df['date'].min().date()
        max_date = analytics_df['date'].max().date()
        selected_date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )

        # Apply filters
        filtered_df = apply_filters(analytics_df, selected_client, selected_platform, selected_date_range)


    # Display sections if filtered data is available or if original df was just empty
    if 'filtered_df' in locals() and (not filtered_df.empty or analytics_df.empty):
         if filtered_df.empty and not analytics_df.empty:
              st.warning("No data matches the selected filters.")
         elif not filtered_df.empty:
              st.subheader("Filtered Data Preview")
              st.dataframe(filtered_df.head())

         # Display key metrics, average metrics chart, visualizations, and reports
         # Pass filtered_df to all display functions
         display_key_metrics(filtered_df)
         display_visualizations(filtered_df)
         display_report_charts(filtered_df)


if __name__ == "__main__":
    main()
