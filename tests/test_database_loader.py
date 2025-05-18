import pytest
import pandas as pd
import sqlite3
import os

# Import the function to be tested
from src.loading.database_loader import load_to_sqlite

# Pytest fixture for creating a temporary directory for test files
# This ensures tests are isolated and don't create files in your project directory
@pytest.fixture
def temp_db_path(tmp_path):
    """Provides a temporary path for a SQLite database file."""
    db_dir = tmp_path / "data" # Create a 'data' subdirectory within the temp dir
    db_dir.mkdir()
    return db_dir / "test_analytics.db"

# --- Test Cases ---

def test_load_to_sqlite_success(temp_db_path):
    """Tests successful loading of a DataFrame to SQLite."""
    # Create a dummy DataFrame
    dummy_df = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['A', 'B', 'C'],
        'date_col': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
    })
    table_name = 'test_table'

    # Call the function to load data
    load_to_sqlite(dummy_df, str(temp_db_path), table_name)

    # Assert that the database file was created
    assert temp_db_path.exists()

    # Connect to the database and read the data back
    conn = None
    try:
        conn = sqlite3.connect(str(temp_db_path))
        # FIX: Add parse_dates to convert the date column back to datetime
        read_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn, parse_dates=['date_col'])

        # Assert that the data read back is equal to the original DataFrame
        # Use pandas testing utility for DataFrame comparison
        pd.testing.assert_frame_equal(read_df, dummy_df)

    except Exception as e:
        pytest.fail(f"An error occurred during verification: {e}")
    finally:
        # Ensure the connection is closed
        if 'conn' in locals() and conn:
            conn.close()

def test_load_to_sqlite_empty_dataframe(temp_db_path):
    """Tests loading an empty DataFrame to SQLite."""
    # Create an empty DataFrame with defined columns
    empty_df = pd.DataFrame(columns=['col1', 'col2', 'date_col'])
    table_name = 'empty_table'

    # Call the function to load data
    load_to_sqlite(empty_df, str(temp_db_path), table_name)

    # Assert that the database file was created (it should be, even if empty)
    assert temp_db_path.exists()

    # Connect and read data back
    conn = None
    try:
        conn = sqlite3.connect(str(temp_db_path))
        # FIX: Corrected typo from table_table_name to table_name
        read_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn, parse_dates=['date_col'])

        # Assert that the DataFrame read back is empty and has the correct columns
        assert read_df.empty
        assert list(read_df.columns) == list(empty_df.columns)
        # Optional: check dtypes if needed, but for empty df column order is often sufficient

    except Exception as e:
        pytest.fail(f"An error occurred during verification: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def test_load_to_sqlite_directory_creation(tmp_path):
    """Tests that the function creates the directory if it doesn't exist."""
    non_existent_dir = tmp_path / "non_existent_data_dir"
    db_path = non_existent_dir / "test_analytics_dir_create.db"
    table_name = 'test_table'

    dummy_df = pd.DataFrame({'col1': [1]})

    # Call the function
    load_to_sqlite(dummy_df, str(db_path), table_name)

    # Assert that the directory was created
    assert non_existent_dir.exists()
    assert non_existent_dir.is_dir()

    # Assert that the database file was created inside the directory
    assert db_path.exists()

