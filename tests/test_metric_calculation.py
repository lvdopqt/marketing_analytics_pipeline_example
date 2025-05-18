import pytest
import pandas as pd
import numpy as np

# Import the functions to be tested
from src.transformation.metric_calculation import (
    calculate_key_metrics,
    summarize_metrics,
    ensure_columns,
    calculate_ctr,
    calculate_cpc,
    calculate_cpm,
    ensure_rate_columns,
    calculate_total_interactions
)

# Helper function to create a dummy DataFrame for testing
def create_dummy_df_for_metrics():
    """Creates a dummy DataFrame with various scenarios for metric calculation."""
    data = {
        'client_id': ['C101', 'C101', 'C102', 'C103', 'C101', 'C102', 'C103', 'C101'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01', '2024-01-01', '2024-01-02', '2024-01-02', '2024-01-02', '2024-01-02']),
        'platform': ['google_ads', 'facebook_ads', 'google_ads', 'web_traffic', 'email_campaigns', 'facebook_ads', 'web_traffic', 'google_ads'],
        'clicks': [100, 80, 0, 0, 50, 120, 0, 150],
        'impressions': [1000, 1600, 500, 0, 0, 2000, 0, 1500], # Include 0 impressions
        'spend_usd': [50.0, 40.0, 0.0, 0.0, 0.0, 60.0, 0.0, 75.0], # Include 0 spend
        'emails_sent': [0, 0, 0, 0, 5000, 0, 0, 0],
        'sessions': [0, 0, 0, 250, 0, 0, 350, 0],
        'open_rate': [0.0, 0.0, 0.0, 0.0, 0.2, 0.0, 0.0, 0.0], # Email specific
        'click_rate': [0.0, 0.0, 0.0, 0.0, 0.05, 0.0, 0.0, 0.0], # Email specific
        'bounce_rate': [0.0, 0.0, 0.0, 0.3, 0.0, 0.0, 0.25, 0.0], # Web specific
        # Add columns that might be missing in some sources
        'campaign_id': ['G1', 'FB1', 'G2', None, 'E1', 'FB2', None, 'G1'],
        'device_type': ['mobile', None, 'desktop', None, None, 'mobile', None, 'desktop'],
        'geo': ['US', 'US', 'CA', 'UK', 'US', 'CA', 'UK', 'US'],
        'avg_session_duration_seconds': [0, 0, 0, 90, 0, 0, 120, 0], # Web specific
        'platform_detail': [None, 'Facebook', None, None, None, 'Instagram', None, None] # FB specific
    }
    df = pd.DataFrame(data)
    return df

# --- Test Cases for Helper Functions ---

def test_ensure_columns():
    """Tests ensure_columns function."""
    df_missing = pd.DataFrame({
        'client_id': ['C101'],
        'date': ['2024-01-01'],
        'spend_usd': [100.0]
        # Missing clicks, impressions, emails_sent, sessions
    })
    cleaned_df = ensure_columns(df_missing)

    # Assert required columns are added and filled with 0
    for col in ['clicks', 'impressions', 'emails_sent', 'sessions']:
        assert col in cleaned_df.columns
        assert (cleaned_df[col] == 0).all()
        assert pd.api.types.is_numeric_dtype(cleaned_df[col]) # Check if numeric

    # Assert existing numeric column is still correct and numeric
    assert 'spend_usd' in cleaned_df.columns
    assert (cleaned_df['spend_usd'] == 100.0).all()
    assert pd.api.types.is_numeric_dtype(cleaned_df['spend_usd'])

def test_calculate_ctr():
    """Tests calculate_ctr function."""
    df = pd.DataFrame({
        'clicks': [100, 50, 0, 200],
        'impressions': [1000, 500, 0, 4000]
    })
    # FIX: Set name attribute for expected Series
    expected_ctr = pd.Series([10.0, 10.0, 0.0, 5.0], name='ctr')
    calculated_ctr = calculate_ctr(df)
    pd.testing.assert_series_equal(calculated_ctr, expected_ctr)

def test_calculate_cpc():
    """Tests calculate_cpc function."""
    df = pd.DataFrame({
        'spend_usd': [50.0, 25.0, 10.0, 0.0, 5.0],
        'clicks': [10, 5, 0, 0, 0] # Include 0 clicks
    })
    # Expected CPC: 50/10=5.0, 25/5=5.0, 10/0=inf -> 0.0, 0/0=NaN -> 0.0, 5/0=inf -> 0.0
    # FIX: Set name attribute for expected Series
    expected_cpc = pd.Series([5.0, 5.0, 0.0, 0.0, 0.0], name='cpc_usd')
    calculated_cpc = calculate_cpc(df)
    pd.testing.assert_series_equal(calculated_cpc, expected_cpc)

def test_calculate_cpm():
    """Tests calculate_cpm function."""
    df = pd.DataFrame({
        'spend_usd': [50.0, 25.0, 10.0, 0.0, 5.0],
        'impressions': [1000, 500, 0, 0, 0] # Include 0 impressions
    })
    # Expected CPM: (50/1000)*1000=50.0, (25/500)*1000=50.0, (10/0)*1000=inf -> 0.0, 0/0=NaN -> 0.0, 5/0=inf -> 0.0
    # FIX: Set name attribute for expected Series
    expected_cpm = pd.Series([50.0, 50.0, 0.0, 0.0, 0.0], name='cpm_usd')
    calculated_cpm = calculate_cpm(df)
    pd.testing.assert_series_equal(calculated_cpm, expected_cpm)

def test_ensure_rate_columns():
    """Tests ensure_rate_columns function."""
    df_missing_rates = pd.DataFrame({
        'client_id': ['C101'],
        'date': ['2024-01-01'],
        'open_rate': [0.1] # Missing click_rate, bounce_rate
    })
    cleaned_df = ensure_rate_columns(df_missing_rates)

    # Assert rate columns are added and filled with 0.0
    for col in ['click_rate', 'bounce_rate']:
        assert col in cleaned_df.columns
        assert (cleaned_df[col] == 0.0).all()
        assert pd.api.types.is_float_dtype(cleaned_df[col]) # Check if float

    # Assert existing rate column is still correct and float
    assert 'open_rate' in cleaned_df.columns
    assert (cleaned_df['open_rate'] == 0.1).all()
    assert pd.api.types.is_float_dtype(cleaned_df['open_rate'])


def test_calculate_total_interactions():
    """Tests calculate_total_interactions function."""
    df = pd.DataFrame({
        'clicks': [100, 0, 50],
        'emails_sent': [0, 5000, 0],
        'sessions': [0, 0, 250],
        'other_col': [1, 2, 3] # Ensure other columns don't interfere
    })
    # FIX: Set name attribute for expected Series
    expected_interactions = pd.Series([100, 5000, 300], name=None) # name=None is the default for sum axis=1
    calculated_interactions = calculate_total_interactions(df)
    pd.testing.assert_series_equal(calculated_interactions, expected_interactions)


# --- Test Cases for Main Functions ---

# FIX: Removed the duplicate test_calculate_key_metrics function

def test_summarize_metrics():
    """Tests the summarize_metrics function."""
    # Create a DataFrame with calculated metrics
    metrics_df = pd.DataFrame({
        'ctr': [10.0, 5.0, 0.0, 10.0, 0.0],
        'cpc_usd': [0.5, 0.8, 0.0, 0.0, 1.0],
        'cpm_usd': [50.0, 80.0, 0.0, 0.0, 100.0],
        'open_rate': [0.2, 0.0, 0.3, 0.0, 0.0],
        'click_rate': [0.05, 0.0, 0.07, 0.0, 0.0],
        'bounce_rate': [0.0, 0.0, 0.0, 0.4, 0.0],
        'total_interactions': [100, 80, 5000, 250, 120]
    })

    summary_series = summarize_metrics(metrics_df)

    # Calculate expected averages manually
    expected_summary = pd.Series({
        'avg_ctr': metrics_df['ctr'].mean(),
        'avg_cpc_usd': metrics_df['cpc_usd'].mean(),
        'avg_cpm_usd': metrics_df['cpm_usd'].mean(),
        'avg_open_rate': metrics_df['open_rate'].mean(),
        'avg_click_rate': metrics_df['click_rate'].mean(),
        'avg_bounce_rate': metrics_df['bounce_rate'].mean(),
        'avg_total_interactions': metrics_df['total_interactions'].mean(),
    })

    # Use pandas testing utility for Series comparison
    # Use rtol and atol for floating point comparisons
    pd.testing.assert_series_equal(summary_series, expected_summary, rtol=1e-4, atol=1e-4)

def test_summarize_metrics_empty_df():
    """Tests summarize_metrics with an empty DataFrame."""
    empty_df = pd.DataFrame(columns=[
        'ctr', 'cpc_usd', 'cpm_usd', 'open_rate', 'click_rate',
        'bounce_rate', 'total_interactions'
    ])

    summary_series = summarize_metrics(empty_df)

    # Expected averages for an empty DataFrame should be NaN
    expected_summary = pd.Series({
        'avg_ctr': np.nan,
        'avg_cpc_usd': np.nan,
        'avg_cpm_usd': np.nan,
        'avg_open_rate': np.nan,
        'avg_click_rate': np.nan,
        'avg_bounce_rate': np.nan,
        'avg_total_interactions': np.nan,
    })

    pd.testing.assert_series_equal(summary_series, expected_summary)

def test_summarize_metrics_missing_cols():
    """Tests summarize_metrics with DataFrame missing some metric columns."""
    # FIX: Create a DataFrame that simulates the output AFTER calculate_key_metrics
    # but with some columns having NaN values.
    df_with_some_nans = pd.DataFrame({
        'client_id': ['C1', 'C2'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-01']),
        'platform': ['google_ads', 'web_traffic'],
        'clicks': [100, 0],
        'impressions': [1000, 0],
        'spend_usd': [50.0, 0.0],
        'emails_sent': [0, 0],
        'sessions': [0, 500],
        'open_rate': [0.0, np.nan], # Simulate missing/NaN rate
        'click_rate': [0.0, np.nan], # Simulate missing/NaN rate
        'bounce_rate': [np.nan, 0.2], # Simulate missing/NaN rate
        # Columns that would be calculated by calculate_key_metrics:
        'ctr': [10.0, 0.0], # 100/1000*100, 0/0=0
        'cpc_usd': [0.5, 0.0], # 50/100, 0/0=0
        'cpm_usd': [50.0, 0.0], # 50/1000*1000, 0/0=0
        'total_interactions': [100, 500] # clicks+emails+sessions
    })
    # Ensure rate columns are float type to accept NaN
    df_with_some_nans[['open_rate', 'click_rate', 'bounce_rate']] = df_with_some_nans[['open_rate', 'click_rate', 'bounce_rate']].astype(float)


    summary_series = summarize_metrics(df_with_some_nans)

    # Calculate expected averages manually based on df_with_some_nans
    # summarize_metrics should handle NaNs correctly (mean ignores NaNs by default)
    expected_summary = pd.Series({
        'avg_ctr': df_with_some_nans['ctr'].mean(), # (10.0 + 0.0) / 2 = 5.0
        'avg_cpc_usd': df_with_some_nans['cpc_usd'].mean(), # (0.5 + 0.0) / 2 = 0.25
        'avg_cpm_usd': df_with_some_nans['cpm_usd'].mean(), # (50.0 + 0.0) / 2 = 25.0
        'avg_open_rate': df_with_some_nans['open_rate'].mean(), # (0.0 + NaN) / 1 = 0.0
        'avg_click_rate': df_with_some_nans['click_rate'].mean(), # (0.0 + NaN) / 1 = 0.0
        'avg_bounce_rate': df_with_some_nans['bounce_rate'].mean(), # (NaN + 0.2) / 1 = 0.2
        'avg_total_interactions': df_with_some_nans['total_interactions'].mean(), # (100 + 500) / 2 = 300.0
    })

    pd.testing.assert_series_equal(summary_series, expected_summary, rtol=1e-4, atol=1e-4)

# FIX: Corrected test function name and removed incorrect assertion
def test_calculate_key_metrics_adds_columns_and_values():
    """Tests that calculate_key_metrics adds expected columns and calculates values correctly."""
    dummy = pd.DataFrame({
        'client_id': ['C101', 'C101', 'C101', 'C102', 'C102', 'C103'],
        'date': pd.to_datetime(['2024-01-01'] * 6),
        'platform': ['google_ads', 'facebook_ads', 'email_campaigns', 'google_ads', 'email_campaigns', 'web_traffic'],
        'campaign_id': ['G1', 'FB1', 'E1', 'G2', 'E2', None],
        'clicks': [100, 80, 50, 200, 0, 0],
        'impressions': [1000, 1200, 0, 2000, 0, 0],
        'spend_usd': [50.5, 40.0, 0.0, 100.2, 0.0, 0.0],
        'emails_sent': [0, 0, 5000, 0, 7000, 0],
        'open_rate': [0.0, 0.0, 0.2, 0.0, 0.25, 0.0],
        'click_rate': [0.0, 0.0, 0.05, 0.0, 0.07, 0.0],
        'pageviews': [0, 0, 0, 0, 0, 300],
        'sessions': [0, 0, 0, 0, 0, 250],
        'bounce_rate': [0.0, 0.0, 0.0, 0.0, 0.0, 0.3],
        'avg_session_duration_seconds': [0, 0, 0, 0, 0, 90],
        'name': ['Acme', 'Acme', 'Acme', 'Bright', 'Bright', 'Green'],
        'industry': ['Retail', 'Retail', 'Retail', 'SaaS', 'SaaS', 'Hospitality']
    })

    result_df = calculate_key_metrics(dummy)

    # Ensure the new metric columns are included
    expected_metric_cols = ['ctr', 'cpc_usd', 'cpm_usd', 'total_interactions']
    for col in expected_metric_cols:
        assert col in result_df.columns
        assert pd.api.types.is_numeric_dtype(result_df[col]) # Ensure they are numeric

    # Spot check known values for the first row (Google Ads)
    first_row = result_df.iloc[0]
    assert first_row['clicks'] == 100
    assert first_row['impressions'] == 1000
    assert round(first_row['ctr'], 2) == 10.0  # 100 clicks / 1000 impressions * 100
    # FIX: Use pytest.approx for floating point comparison
    assert first_row['cpc_usd'] == pytest.approx(0.505)  # $50.5 / 100 clicks
    assert round(first_row['cpm_usd'], 2) == 50.5  # ($50.5 / 1000) * 1000
    assert first_row['total_interactions'] == 100 # clicks + 0 emails + 0 sessions

    # Add more spot checks for other rows/scenarios if desired

    # Example: Row with 0 impressions (Email Campaigns)
    zero_impressions_row = result_df[result_df['platform'] == 'email_campaigns'].iloc[0]
    assert zero_impressions_row['impressions'] == 0
    assert zero_impressions_row['ctr'] == 0.0
    assert zero_impressions_row['cpm_usd'] == 0.0
    assert zero_impressions_row['total_interactions'] == 5050 # 50 clicks + 5000 emails + 0 sessions

    # Example: Row with 0 clicks (Email Campaigns - E2)
    zero_clicks_row = result_df[result_df['campaign_id'] == 'E2'].iloc[0]
    assert zero_clicks_row['clicks'] == 0
    assert zero_clicks_row['cpc_usd'] == 0.0
    assert zero_clicks_row['total_interactions'] == 7000 # 0 clicks + 7000 emails + 0 sessions


    # Example: Web traffic row (check total_interactions)
    web_row = result_df[result_df['platform'] == 'web_traffic'].iloc[0]
    assert web_row['clicks'] == 0
    assert web_row['emails_sent'] == 0
    assert web_row['sessions'] == 250
    assert web_row['total_interactions'] == 250 # 0 clicks + 0 emails + 250 sessions

