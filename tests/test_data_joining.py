import pytest
import pandas as pd
import numpy as np # Import numpy for NaN comparison

# Import the function to be tested
from src.transformation.data_joining import join_marketing_data, EXPECTED_FACT_COLUMNS, CLIENT_COLUMNS

# Helper function to create a dummy clients DataFrame
def create_dummy_clients_df():
    return pd.DataFrame({
        'client_id': ['C101', 'C102', 'C103'],
        'name': ['Acme Corp', 'Bright Ideas', 'Green Solutions'],
        'industry': ['Retail', 'SaaS', 'Hospitality'],
        'account_manager': ['Manager A', 'Manager B', 'Manager C'],
        'signup_date': pd.to_datetime(['2023-01-01', '2022-05-15', '2024-02-20'])
    })

# Helper function to create a dummy fact DataFrame (simulating cleaned ingestion output)
def create_dummy_fact_df(platform_name: str, client_id: str, date: str, **kwargs):
    """Creates a basic dummy fact row for a given platform."""
    base_data = {
        'client_id': [client_id],
        'date': [pd.to_datetime(date)],
        'platform': [platform_name],
        # Include common columns with default values
        'campaign_id': [None], 'device_type': [None], 'geo': [None],
        'clicks': [0], 'impressions': [0], 'spend_usd': [0.0],
        'emails_sent': [0], 'open_rate': [0.0], 'click_rate': [0.0], 'subject_line': [None],
        'pageviews': [0], 'sessions': [0], 'bounce_rate': [0.0], 'avg_session_duration_seconds': [0],
        'platform_detail': [None]
    }
    # Update with specific values provided
    base_data.update({k: [v] for k, v in kwargs.items()})

    # Ensure all EXPECTED_FACT_COLUMNS are present, even if None
    for col in EXPECTED_FACT_COLUMNS:
        if col not in base_data:
            base_data[col] = [None] # Or [0] for numeric, [0.0] for float

    # Create DataFrame and reorder columns to match EXPECTED_FACT_COLUMNS
    df = pd.DataFrame(base_data)
    # Filter for columns that actually exist in the dataframe before reordering
    existing_expected_cols = [col for col in EXPECTED_FACT_COLUMNS if col in df.columns]
    df = df[existing_expected_cols]

    return df


# --- Test Cases ---

def test_join_basic():
    """Tests a basic successful join scenario."""
    cleaned_data = {
        'google_ads': create_dummy_fact_df(
            'google_ads', 'C101', '2024-01-01',
            clicks=100, impressions=1000, spend_usd=50.0,
            campaign_id='G1', device_type='mobile', geo='US'
        ),
        'clients': create_dummy_clients_df().iloc[[0]] # Use only C101 client
    }

    result = join_marketing_data(cleaned_data)

    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1

    # Check joined columns and values
    assert 'name' in result.columns
    assert 'industry' in result.columns
    assert result.loc[0, 'client_id'] == 'C101'
    assert result.loc[0, 'name'] == 'Acme Corp'
    assert result.loc[0, 'platform'] == 'google_ads'
    assert result.loc[0, 'clicks'] == 100 # Check a fact metric


def test_join_multiple_fact_sources():
    """Tests joining data from multiple fact sources."""
    cleaned_data = {
        'google_ads': create_dummy_fact_df('google_ads', 'C101', '2024-01-01', clicks=100),
        'facebook_ads': create_dummy_fact_df('facebook_ads', 'C101', '2024-01-01', spend_usd=40.0),
        'web_traffic': create_dummy_fact_df('web_traffic', 'C102', '2024-01-02', sessions=250),
        'clients': create_dummy_clients_df().iloc[[0, 1]] # Use C101 and C102 clients
    }

    result = join_marketing_data(cleaned_data)

    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3 # 3 rows from the fact dataframes

    # Check rows and joined data
    google_row = result[result['platform'] == 'google_ads'].iloc[0]
    assert google_row['client_id'] == 'C101'
    assert google_row['name'] == 'Acme Corp'
    assert google_row['clicks'] == 100
    assert google_row['spend_usd'] == 0.0 # spend_usd was 0 in google_ads dummy

    facebook_row = result[result['platform'] == 'facebook_ads'].iloc[0]
    assert facebook_row['client_id'] == 'C101'
    assert facebook_row['name'] == 'Acme Corp'
    assert facebook_row['spend_usd'] == 40.0 # spend_usd was 40.0 in facebook_ads dummy
    assert facebook_row['clicks'] == 0 # clicks was 0 in facebook_ads dummy

    web_row = result[result['platform'] == 'web_traffic'].iloc[0]
    assert web_row['client_id'] == 'C102'
    assert web_row['name'] == 'Bright Ideas'
    assert web_row['sessions'] == 250


def test_join_with_client_not_found():
    """Tests that rows with client_ids not in the clients table are kept but client info is filled with 'Unknown'."""
    cleaned_data = {
        'google_ads': create_dummy_fact_df('google_ads', 'C999', '2024-01-01', clicks=100), # C999 not in clients
        'clients': create_dummy_clients_df().iloc[[0]] # Only C101
    }

    result = join_marketing_data(cleaned_data)

    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1 # The row should be kept

    # Check that client info columns are filled with 'Unknown' for the unmatched row
    assert result.loc[0, 'client_id'] == 'C999'
    # FIX: Assert that the columns are 'Unknown' instead of checking for NaN
    assert result.loc[0, 'name'] == 'Unknown'
    assert result.loc[0, 'industry'] == 'Unknown'
    # Add checks for other client info columns if needed


def test_join_with_empty_fact_data():
    """Tests joining when there are no non-empty fact dataframes."""
    cleaned_data = {
        'google_ads': pd.DataFrame(), # Empty DataFrame
        'facebook_ads': None, # None entry
        'clients': create_dummy_clients_df() # Valid clients data
    }

    result = join_marketing_data(cleaned_data)

    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert result.empty # Resulting DataFrame should be empty

    # Check that the empty DataFrame has the expected columns from both fact and client tables
    expected_cols = EXPECTED_FACT_COLUMNS + [col for col in CLIENT_COLUMNS if col != 'client_id']
    assert list(result.columns) == expected_cols


def test_join_with_missing_clients_data():
    """Tests joining when the clients DataFrame is missing or empty."""
    cleaned_data_missing = {
        'google_ads': create_dummy_fact_df('google_ads', 'C101', '2024-01-01', clicks=100),
        # clients key is missing
    }
    result_missing = join_marketing_data(cleaned_data_missing)
    assert result_missing is None # Should return None if clients is missing

    cleaned_data_empty = {
        'google_ads': create_dummy_fact_df('google_ads', 'C101', '2024-01-01', clicks=100),
        'clients': pd.DataFrame() # Empty clients DataFrame
    }
    result_empty = join_marketing_data(cleaned_data_empty)
    assert result_empty is None # Should return None if clients is empty