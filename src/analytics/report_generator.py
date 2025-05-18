import pandas as pd
import logging
import os
import sqlite3

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _load_data(data_path: str, data_format: str, table_name: str | None = None) -> pd.DataFrame | None:
    """
    Internal helper to load data from the specified source (SQLite or Parquet).
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
            logging.info(f"Successfully loaded {len(df)} records from SQLite table '{table_name}'.")
        elif data_format == 'parquet':
            if not os.path.exists(data_path):
                 logging.error(f"Parquet file not found at {data_path}")
                 return None
            df = pd.read_parquet(data_path)
            logging.info(f"Successfully loaded {len(df)} records from Parquet file.")
        else:
            logging.error(f"Unsupported data format specified: {data_format}")
            return None

        # Ensure date column is datetime for consistent handling
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.dropna(subset=['date'], inplace=True) # Drop rows with invalid dates

        return df

    except Exception as e:
        logging.error(f"An error occurred during data loading for reports: {e}")
        return None

def _generate_daily_client_spend(df: pd.DataFrame, output_dir: str):
    """Generates the Daily Client Spend report."""
    if 'spend_usd' in df.columns and 'client_id' in df.columns and 'date' in df.columns:
        daily_client_spend = df.groupby(['date', 'client_id'])['spend_usd'].sum().reset_index()
        spend_report_path = os.path.join(output_dir, 'daily_client_spend_report.csv')
        daily_client_spend.to_csv(spend_report_path, index=False)
        logging.info(f"Generated daily client spend report: {spend_report_path}")
    else:
        logging.warning("Skipping Daily Client Spend report: Missing required columns.")

def _generate_total_clicks_by_platform(df: pd.DataFrame, output_dir: str):
    """Generates the Total Clicks by Platform report."""
    if 'clicks' in df.columns and 'platform' in df.columns:
        total_clicks_by_platform = df.groupby('platform')['clicks'].sum().reset_index()
        clicks_report_path = os.path.join(output_dir, 'total_clicks_by_platform_report.csv')
        total_clicks_by_platform.to_csv(clicks_report_path, index=False)
        logging.info(f"Generated total clicks by platform report: {clicks_report_path}")
    else:
         logging.warning("Skipping Total Clicks by Platform report: Missing required columns.")

def _generate_ctr_trends(df: pd.DataFrame, output_dir: str):
    """Generates the CTR Trends Over Time report."""
    if 'clicks' in df.columns and 'impressions' in df.columns and 'date' in df.columns:
         # Filter for platforms where Impressions are relevant (Google, Facebook)
         ad_platforms_df = df[df['platform'].isin(['google_ads', 'facebook_ads'])].copy()
         if not ad_platforms_df.empty:
              daily_ad_performance = ad_platforms_df.groupby('date').agg(
                  total_clicks=('clicks', 'sum'),
                  total_impressions=('impressions', 'sum')
              ).reset_index()

              # Calculate daily aggregate CTR
              daily_ad_performance['daily_ctr'] = 0.0
              valid_impressions_mask = daily_ad_performance['total_impressions'] > 0
              daily_ad_performance.loc[valid_impressions_mask, 'daily_ctr'] = (
                  (daily_ad_performance.loc[valid_impressions_mask, 'total_clicks'] / daily_ad_performance.loc[valid_impressions_mask, 'total_impressions']) * 100
              )

              ctr_trend_report_path = os.path.join(output_dir, 'ctr_trends_report.csv')
              daily_ad_performance.to_csv(ctr_trend_report_path, index=False)
              logging.info(f"Generated CTR trends report: {ctr_trend_report_path}")
         else:
              logging.warning("No data from Google Ads or Facebook Ads to calculate CTR trends.")
    else:
         logging.warning("Skipping CTR Trends report: Missing required columns (clicks, impressions, date).")

def _generate_campaign_summary(df: pd.DataFrame, output_dir: str):
    """Generates the Aggregated Campaign Performance report."""
    if 'client_id' in df.columns and 'campaign_id' in df.columns and 'spend_usd' in df.columns:
        campaign_summary = df.groupby(['client_id', 'campaign_id']).agg(
            total_spend=('spend_usd', 'sum'),
            total_clicks=('clicks', 'sum'),
            total_impressions=('impressions', 'sum')
        ).reset_index()
        campaign_summary_path = os.path.join(output_dir, 'campaign_summary_report.csv')
        campaign_summary.to_csv(campaign_summary_path, index=False)
        logging.info(f"Generated campaign summary report: {campaign_summary_path}")
    else:
         logging.warning("Skipping Campaign Summary report: Missing required columns.")


def generate_summary_reports(data_path: str, output_dir: str, data_format: str = 'sqlite', table_name: str = 'marketing_analytics'):
    """
    Generates summary reports from the final processed data.

    Args:
        data_path: Path to the final data source (SQLite DB file or Parquet file).
        output_dir: Directory where the generated reports (CSV files) will be saved.
        data_format: Format of the data source ('sqlite' or 'parquet').
        table_name: Table name if data_format is 'sqlite'.
    """
    logging.info(f"Starting summary report generation process from {data_path}")

    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Created output directory for reports: {output_dir}")

    # Load the final data
    df = _load_data(data_path, data_format, table_name)

    if df is None or df.empty:
        logging.warning("No data loaded for report generation. Skipping reports.")
        return # Stop if no data was loaded

    logging.info(f"Loaded {len(df)} records for reporting.")

    # Generate reports using helper functions
    _generate_daily_client_spend(df, output_dir)
    _generate_total_clicks_by_platform(df, output_dir)
    _generate_ctr_trends(df, output_dir)
    _generate_campaign_summary(df, output_dir)

    logging.info("Summary report generation finished.")