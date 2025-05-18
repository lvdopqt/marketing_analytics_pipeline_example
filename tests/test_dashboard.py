import pytest
import pandas as pd
import os
import sqlite3
import pyarrow.parquet as pq # Needed to save dummy parquet

# Import the functions to be tested
from src.analytics.dashboard import load_analytics_data, apply_filters

# Helper function to create a dummy DataFrame simulating analytics data
def create_dummy_analytics_df():
    """Creates a dummy DataFrame simulating the final analytics data structure."""
    data = {
        'client_id': ['C101', 'C101', 'C102', 'C103', 'C101', 'C102', 'C101'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-01', '2024-01-02', '2024-01-01', '2024-01-03']),
        'platform': ['google_ads', 'google_ads', 'facebook_ads', 'web_traffic', 'email_campaigns', 'google_ads', 'facebook_ads'],
        'clicks': [100, 120, 80, 0, 50, 150, 90],
        'impressions': [1000, 1100, 1500, 0, 0, 2000, 1600],
        'spend_usd': [50.5, 60.0, 45.0, 0.0, 0.0, 80.0, 55.0],
        'emails_sent': [0, 0, 0, 0, 5000, 0, 0],
        'sessions': [0, 0, 0, 300, 0, 0, 0],
        'name': ['Acme', 'Acme', 'Bright', 'Green', 'Acme', 'Bright', 'Acme'],
        'industry': ['Retail', 'Retail', 'SaaS', 'Hospitality', 'Retail', 'SaaS', 'Retail'],
        'ctr': [10.0, 10.91, 5.33, 0.0, 0.0, 7.5, 5.63],
        'attributed_revenue_from_source_usd': [150.0, 0.0, 100.0, 50.0, 75.0, 0.0, 120.0]
    }
    df = pd.DataFrame(data)
    return df

# Fixture to create a dummy SQLite DB file for testing
@pytest.fixture
def dummy_sqlite_db_for_dashboard(tmp_path):
    """Creates a temporary SQLite DB file with dummy data for dashboard testing."""
    db_dir = tmp_path / "data"
    db_dir.mkdir()
    db_path = db_dir / "test_dashboard_analytics.db"
    table_name = "marketing_analytics"

    dummy_df = create_dummy_analytics_df()

    conn = None
    try:
        conn = sqlite3.connect(str(db_path))
        dummy_df.to_sql(table_name, conn, if_exists='replace', index=False)
    except Exception as e:
        pytest.fail(f"Failed to create dummy SQLite DB for dashboard test: {e}")
    finally:
        if conn:
            conn.close()

    return str(db_path), table_name

# Fixture to create a dummy Parquet file for testing
@pytest.fixture
def dummy_parquet_file_for_dashboard(tmp_path):
    """Creates a temporary Parquet file with dummy data for dashboard testing."""
    processed_dir = tmp_path / "data" # Use 'data' to match dashboard path logic
    processed_dir.mkdir()
    parquet_path = processed_dir / "test_dashboard_analytics.parquet"

    dummy_df = create_dummy_analytics_df()

    try:
        dummy_df.to_parquet(str(parquet_path), index=False)
    except Exception as e:
        pytest.fail(f"Failed to create dummy Parquet file for dashboard test: {e}")

    return str(parquet_path)

# --- Test Cases for load_analytics_data ---

def test_load_analytics_data_from_sqlite_success(dummy_sqlite_db_for_dashboard):
    """Tests successful loading from SQLite."""
    db_path, table_name = dummy_sqlite_db_for_dashboard
    loaded_df = load_analytics_data(db_path, 'sqlite', table_name)

    assert loaded_df is not None
    assert not loaded_df.empty
    # Basic check for expected columns
    expected_cols = ['client_id', 'date', 'platform', 'clicks', 'spend_usd']
    for col in expected_cols:
        assert col in loaded_df.columns

    # Check date column type
    assert pd.api.types.is_datetime64_any_dtype(loaded_df['date'])

def test_load_analytics_data_from_parquet_success(dummy_parquet_file_for_dashboard):
    """Tests successful loading from Parquet."""
    parquet_path = dummy_parquet_file_for_dashboard
    loaded_df = load_analytics_data(parquet_path, 'parquet')

    assert loaded_df is not None
    assert not loaded_df.empty
    # Basic check for expected columns
    expected_cols = ['client_id', 'date', 'platform', 'clicks', 'spend_usd']
    for col in expected_cols:
        assert col in loaded_df.columns

    # Check date column type
    assert pd.api.types.is_datetime64_any_dtype(loaded_df['date'])


def test_load_analytics_data_file_not_found():
    """Tests loading when the file does not exist."""
    non_existent_path = "non_existent/path/fake.db"
    loaded_df = load_analytics_data(non_existent_path, 'sqlite', 'fake_table')
    assert loaded_df is None # Should return None

    non_existent_path_parquet = "non_existent/path/fake.parquet"
    loaded_df_parquet = load_analytics_data(non_existent_path_parquet, 'parquet')
    assert loaded_df_parquet is None # Should return None


def test_load_analytics_data_empty_db_table(tmp_path):
    """Tests loading from an empty SQLite table."""
    db_dir = tmp_path / "data"
    db_dir.mkdir()
    empty_db_path = db_dir / "empty_test.db"
    empty_table_name = "empty_table"

    # Create an empty table with some columns
    conn = sqlite3.connect(str(empty_db_path))
    conn.execute(f"CREATE TABLE {empty_table_name} (col1 TEXT, col2 INTEGER)")
    conn.close()

    loaded_df = load_analytics_data(str(empty_db_path), 'sqlite', empty_table_name)

    assert loaded_df is not None
    assert loaded_df.empty
    assert list(loaded_df.columns) == ['col1', 'col2'] # Check columns are preserved

def test_load_analytics_data_empty_parquet_file(tmp_path):
    """Tests loading from an empty Parquet file."""
    processed_dir = tmp_path / "data"
    processed_dir.mkdir()
    empty_parquet_path = processed_dir / "empty_test.parquet"

    # Create an empty DataFrame with columns and save as parquet
    empty_df = pd.DataFrame(columns=['colA', 'colB'])
    empty_df.to_parquet(str(empty_parquet_path), index=False)

    loaded_df = load_analytics_data(str(empty_parquet_path), 'parquet')

    assert loaded_df is not None
    assert loaded_df.empty
    assert list(loaded_df.columns) == ['colA', 'colB'] # Check columns are preserved


# --- Test Cases for apply_filters ---

def test_apply_filters_no_filters():
    """Tests filtering with no filters applied."""
    dummy_df = create_dummy_analytics_df()
    filtered_df = apply_filters(dummy_df, 'All', 'All', (dummy_df['date'].min().date(), dummy_df['date'].max().date()))
    pd.testing.assert_frame_equal(filtered_df, dummy_df) # Should return the original DataFrame

def test_apply_filters_client():
    """Tests filtering by client_id."""
    dummy_df = create_dummy_analytics_df()
    selected_client = 'C101'
    filtered_df = apply_filters(dummy_df, selected_client, 'All', (dummy_df['date'].min().date(), dummy_df['date'].max().date()))
    assert (filtered_df['client_id'] == selected_client).all()
    assert len(filtered_df) == dummy_df[dummy_df['client_id'] == selected_client].shape[0]

def test_apply_filters_platform():
    """Tests filtering by platform."""
    dummy_df = create_dummy_analytics_df()
    selected_platform = 'google_ads'
    filtered_df = apply_filters(dummy_df, 'All', selected_platform, (dummy_df['date'].min().date(), dummy_df['date'].max().date()))
    assert (filtered_df['platform'] == selected_platform).all()
    assert len(filtered_df) == dummy_df[dummy_df['platform'] == selected_platform].shape[0]

def test_apply_filters_date_range():
    """Tests filtering by date range."""
    dummy_df = create_dummy_analytics_df()
    min_date = dummy_df['date'].min().date()
    max_date = dummy_df['date'].max().date()
    # Select a date range that includes only dates > min_date and < max_date
    start_date = min_date + pd.Timedelta(days=1)
    end_date = max_date - pd.Timedelta(days=1)
    selected_date_range = (start_date, end_date)

    filtered_df = apply_filters(dummy_df, 'All', 'All', selected_date_range)

    # Convert selected_date_range to datetime for comparison
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)

    assert (filtered_df['date'] >= start_dt).all()
    assert (filtered_df['date'] <= end_dt).all()
    expected_len = dummy_df[(dummy_df['date'] >= start_dt) & (dummy_df['date'] <= end_dt)].shape[0]
    assert len(filtered_df) == expected_len

def test_apply_filters_single_date():
    """Tests filtering by a single date."""
    dummy_df = create_dummy_analytics_df()
    single_date = dummy_df['date'].iloc[0].date() # Use the date of the first row
    selected_date_range = (single_date,) # Pass as a tuple with one element

    filtered_df = apply_filters(dummy_df, 'All', 'All', selected_date_range)

    single_dt = pd.to_datetime(single_date)
    assert (filtered_df['date'] == single_dt).all()
    expected_len = dummy_df[dummy_df['date'] == single_dt].shape[0]
    assert len(filtered_df) == expected_len

def test_apply_filters_combination():
    """Tests filtering with a combination of filters."""
    dummy_df = create_dummy_analytics_df()
    selected_client = 'C101'
    selected_platform = 'google_ads'
    min_date = dummy_df['date'].min().date()
    max_date = dummy_df['date'].max().date()
    selected_date_range = (min_date, max_date) # Use full range for simplicity

    filtered_df = apply_filters(dummy_df, selected_client, selected_platform, selected_date_range)

    assert (filtered_df['client_id'] == selected_client).all()
    assert (filtered_df['platform'] == selected_platform).all()
    # Date range check is implicitly covered by the date range fixture if used,
    # or by comparing length to expected filtered length.

    expected_len = dummy_df[(dummy_df['client_id'] == selected_client) & (dummy_df['platform'] == selected_platform)].shape[0]
    assert len(filtered_df) == expected_len

def test_apply_filters_no_match():
    """Tests filtering with criteria that match no data."""
    dummy_df = create_dummy_analytics_df()
    selected_client = 'NonExistentClient'
    filtered_df = apply_filters(dummy_df, selected_client, 'All', (dummy_df['date'].min().date(), dummy_df['date'].max().date()))
    assert filtered_df.empty

    selected_platform = 'NonExistentPlatform'
    filtered_df = apply_filters(dummy_df, 'All', selected_platform, (dummy_df['date'].min().date(), dummy_df['date'].max().date()))
    assert filtered_df.empty

    selected_date_range = (pd.to_datetime('2050-01-01').date(), pd.to_datetime('2050-01-02').date())
    filtered_df = apply_filters(dummy_df, 'All', 'All', selected_date_range)
    assert filtered_df.empty

