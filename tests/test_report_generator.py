import pytest
import pandas as pd
import os
import sqlite3
import pyarrow.parquet as pq

# Import the function to be tested and its internal helpers
from src.analytics.report_generator import generate_summary_reports


# Helper function to create a dummy DataFrame with all necessary columns
def create_dummy_analytics_df():
    """Creates a dummy DataFrame simulating the final analytics data structure."""
    data = {
        'client_id': ['C101', 'C101', 'C102', 'C103', 'C101', 'C102', 'C101'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01', '2024-01-03']),
        'platform': ['google_ads', 'google_ads', 'facebook_ads', 'web_traffic', 'email_campaigns', 'google_ads', 'facebook_ads'],
        'campaign_id': ['G1', 'G1', 'FB1', None, 'E1', 'G2', 'FB1'],
        'clicks': [100, 120, 80, 0, 50, 150, 90],
        'impressions': [1000, 1100, 1500, 0, 0, 2000, 1600],
        'spend_usd': [50.5, 60.0, 45.0, 0.0, 0.0, 80.0, 55.0],
        'emails_sent': [0, 0, 0, 0, 5000, 0, 0],
        'sessions': [0, 0, 0, 300, 0, 0, 0],
        'name': ['Acme', 'Acme', 'Bright', 'Green', 'Acme', 'Bright', 'Acme'],
        'industry': ['Retail', 'Retail', 'SaaS', 'Hospitality', 'Retail', 'SaaS', 'Retail'],
        # Add other columns if they are expected in the final df, e.g., calculated metrics
        'ctr': [10.0, 10.91, 5.33, 0.0, 0.0, 7.5, 5.63], # Example CTRs
        'attributed_revenue_from_source_usd': [150.0, 0.0, 100.0, 50.0, 75.0, 0.0, 120.0] # Example revenue
    }
    df = pd.DataFrame(data)
    # Ensure date column is datetime
    df['date'] = pd.to_datetime(df['date'])
    return df

# Helper function to calculate expected report DataFrame (Daily Client Spend)
def expected_daily_client_spend(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the expected Daily Client Spend DataFrame."""
    if 'spend_usd' in df.columns and 'client_id' in df.columns and 'date' in df.columns:
        return df.groupby(['date', 'client_id'])['spend_usd'].sum().reset_index()
    return pd.DataFrame(columns=['date', 'client_id', 'spend_usd']) # Return empty if cols missing

# Helper function to calculate expected report DataFrame (Total Clicks by Platform)
def expected_total_clicks_by_platform(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the expected Total Clicks by Platform DataFrame."""
    if 'clicks' in df.columns and 'platform' in df.columns:
        return df.groupby('platform')['clicks'].sum().reset_index()
    return pd.DataFrame(columns=['platform', 'clicks'])

# Helper function to calculate expected report DataFrame (CTR Trends)
def expected_ctr_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the expected CTR Trends Over Time DataFrame."""
    if 'clicks' in df.columns and 'impressions' in df.columns and 'date' in df.columns:
        ad_platforms_df = df[df['platform'].isin(['google_ads', 'facebook_ads'])].copy()
        if ad_platforms_df.empty:
            return pd.DataFrame(columns=['date', 'total_clicks', 'total_impressions', 'daily_ctr'])

        daily_ad_performance = ad_platforms_df.groupby('date').agg(
            total_clicks=('clicks', 'sum'),
            total_impressions=('impressions', 'sum')
        ).reset_index()

        daily_ad_performance['daily_ctr'] = 0.0
        valid_impressions_mask = daily_ad_performance['total_impressions'] > 0
        daily_ad_performance.loc[valid_impressions_mask, 'daily_ctr'] = (
            (daily_ad_performance.loc[valid_impressions_mask, 'total_clicks'] / daily_ad_performance.loc[valid_impressions_mask, 'total_impressions']) * 100
        )
        return daily_ad_performance
    return pd.DataFrame(columns=['date', 'total_clicks', 'total_impressions', 'daily_ctr'])

# Helper function to calculate expected report DataFrame (Campaign Summary)
def expected_campaign_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the expected Aggregated Campaign Performance DataFrame."""
    if 'client_id' in df.columns and 'campaign_id' in df.columns and 'spend_usd' in df.columns:
        return df.groupby(['client_id', 'campaign_id']).agg(
            total_spend=('spend_usd', 'sum'),
            total_clicks=('clicks', 'sum'),
            total_impressions=('impressions', 'sum')
        ).reset_index()
    return pd.DataFrame(columns=['client_id', 'campaign_id', 'total_spend', 'total_clicks', 'total_impressions'])


# Pytest fixture for creating a temporary output directory
@pytest.fixture
def temp_output_dir(tmp_path):
    """Provides a temporary path for report output directory."""
    output_dir = tmp_path / "reports"
    output_dir.mkdir()
    return output_dir

# Fixture to create a dummy SQLite DB file
@pytest.fixture
def dummy_sqlite_db(tmp_path):
    """Creates a temporary SQLite DB file with dummy data."""
    db_dir = tmp_path / "data"
    db_dir.mkdir()
    db_path = db_dir / "test_analytics.db"
    table_name = "marketing_analytics"

    dummy_df = create_dummy_analytics_df()

    conn = None
    try:
        conn = sqlite3.connect(str(db_path))
        dummy_df.to_sql(table_name, conn, if_exists='replace', index=False)
    except Exception as e:
        pytest.fail(f"Failed to create dummy SQLite DB: {e}")
    finally:
        if conn:
            conn.close()

    return str(db_path), table_name

# Fixture to create a dummy Parquet file
@pytest.fixture
def dummy_parquet_file(tmp_path):
    """Creates a temporary Parquet file with dummy data."""
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    parquet_path = processed_dir / "test_analytics.parquet"

    dummy_df = create_dummy_analytics_df()

    try:
        dummy_df.to_parquet(str(parquet_path), index=False)
    except Exception as e:
        pytest.fail(f"Failed to create dummy Parquet file: {e}")

    return str(parquet_path)


# --- Test Cases ---

def test_generate_summary_reports_from_sqlite(dummy_sqlite_db, temp_output_dir):
    """Tests report generation from a SQLite database."""
    db_path, table_name = dummy_sqlite_db
    output_dir = str(temp_output_dir)

    # Get the dummy data used to create the DB
    dummy_df = create_dummy_analytics_df()

    # Generate reports
    generate_summary_reports(db_path, output_dir, data_format='sqlite', table_name=table_name)

    # Define expected report file paths
    expected_reports = {
        'daily_client_spend_report.csv': expected_daily_client_spend(dummy_df),
        'total_clicks_by_platform_report.csv': expected_total_clicks_by_platform(dummy_df),
        'ctr_trends_report.csv': expected_ctr_trends(dummy_df),
        'campaign_summary_report.csv': expected_campaign_summary(dummy_df)
    }

    # Verify each report file was created and its content is correct
    for report_file, expected_df in expected_reports.items():
        report_path = os.path.join(output_dir, report_file)
        assert os.path.exists(report_path)

        # Read the generated report CSV
        generated_df = pd.read_csv(report_path)

        # Convert date columns to datetime for comparison if they exist
        if 'date' in expected_df.columns:
             generated_df['date'] = pd.to_datetime(generated_df['date'])

        # Use pandas testing utility to compare DataFrames
        # Check_dtype=False is sometimes needed for CSV reads vs original DF types
        pd.testing.assert_frame_equal(generated_df, expected_df, check_dtype=False)


def test_generate_summary_reports_from_parquet(dummy_parquet_file, temp_output_dir):
    """Tests report generation from a Parquet file."""
    parquet_path = dummy_parquet_file
    output_dir = str(temp_output_dir)

    # Get the dummy data used to create the Parquet file
    dummy_df = create_dummy_analytics_df()

    # Generate reports
    generate_summary_reports(parquet_path, output_dir, data_format='parquet')

    # Define expected report file paths (same as SQLite test)
    expected_reports = {
        'daily_client_spend_report.csv': expected_daily_client_spend(dummy_df),
        'total_clicks_by_platform_report.csv': expected_total_clicks_by_platform(dummy_df),
        'ctr_trends_report.csv': expected_ctr_trends(dummy_df),
        'campaign_summary_report.csv': expected_campaign_summary(dummy_df)
    }

    # Verify each report file was created and its content is correct
    for report_file, expected_df in expected_reports.items():
        report_path = os.path.join(output_dir, report_file)
        assert os.path.exists(report_path)

        # Read the generated report CSV
        generated_df = pd.read_csv(report_path)

        # Convert date columns to datetime for comparison if they exist
        if 'date' in expected_df.columns:
             generated_df['date'] = pd.to_datetime(generated_df['date'])

        # Use pandas testing utility to compare DataFrames
        pd.testing.assert_frame_equal(generated_df, expected_df, check_dtype=False)


def test_generate_summary_reports_empty_data(temp_output_dir):
    """Tests report generation with empty input data."""
    output_dir = str(temp_output_dir)
    # Create an empty DataFrame with expected columns
    empty_df = pd.DataFrame(columns=[
        'client_id', 'date', 'platform', 'campaign_id', 'clicks', 'impressions',
        'spend_usd', 'emails_sent', 'sessions', 'name', 'industry', 'ctr',
        'attributed_revenue_from_source_usd'
    ])

    # Simulate saving empty data to a temporary Parquet file
    temp_parquet_path = temp_output_dir / "empty_analytics.parquet" # Save in output dir for simplicity
    empty_df.to_parquet(str(temp_parquet_path), index=False)


    # Generate reports from the empty data
    generate_summary_reports(str(temp_parquet_path), output_dir, data_format='parquet')

    # Assert that the report files were NOT created (or are empty, depending on your generate_summary_reports logic)
    # Your current logic logs warnings and returns if df is empty, so files should not be created.
    expected_report_files = [
        'daily_client_spend_report.csv',
        'total_clicks_by_platform_report.csv',
        'ctr_trends_report.csv',
        'campaign_summary_report.csv'
    ]
    for report_file in expected_report_files:
        report_path = os.path.join(output_dir, report_file)
        assert not os.path.exists(report_path)


def test_generate_summary_reports_missing_columns(tmp_path, temp_output_dir):
    """Tests report generation with input data missing some required columns."""
    output_dir = str(temp_output_dir)
    # Create a dummy DataFrame missing some columns required for reports
    missing_cols_df = pd.DataFrame({
        'client_id': ['C101', 'C102'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-01']),
        'platform': ['google_ads', 'facebook_ads'],
        'spend_usd': [50.5, 40.0],
        # Missing clicks, impressions, campaign_id
    })

    # Simulate saving data to a temporary Parquet file
    temp_parquet_path = tmp_path / "missing_cols_analytics.parquet"
    missing_cols_df.to_parquet(str(temp_parquet_path), index=False)

    # Generate reports
    generate_summary_reports(str(temp_parquet_path), output_dir, data_format='parquet')

    # Check which reports are expected to be generated (only Daily Client Spend in this case)
    # And which should be skipped (Clicks, CTR, Campaign Summary)
    expected_generated_reports = ['daily_client_spend_report.csv']
    expected_skipped_reports = [
        'total_clicks_by_platform_report.csv',
        'ctr_trends_report.csv',
        'campaign_summary_report.csv'
    ]

    # Verify generated reports
    for report_file in expected_generated_reports:
        report_path = os.path.join(output_dir, report_file)
        assert os.path.exists(report_path)
        # Optional: Add content assertion for the generated report

    # Verify skipped reports were NOT created
    for report_file in expected_skipped_reports:
        report_path = os.path.join(output_dir, report_file)
        assert not os.path.exists(report_path)