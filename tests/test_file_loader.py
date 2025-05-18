import pytest
import pandas as pd
import os

# Import the function to be tested
# Assuming the test file is in tests/loading/ and the source is in src/loading/
from src.loading.file_loader import load_to_parquet

# Pytest fixture for creating a temporary directory for test files
# This ensures tests are isolated and don't create files in your project directory
@pytest.fixture
def temp_parquet_path(tmp_path):
    """Provides a temporary path for a Parquet file."""
    data_dir = tmp_path / "processed" # Create a 'processed' subdirectory within the temp dir
    data_dir.mkdir()
    return data_dir / "test_analytics.parquet"

# --- Test Cases ---

def test_load_to_parquet_success(temp_parquet_path):
    """Tests successful loading of a DataFrame to Parquet."""
    # Create a dummy DataFrame
    dummy_df = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['X', 'Y', 'Z'],
        'value': [10.5, 20.1, 30.9]
    })

    # Call the function to load data
    load_to_parquet(dummy_df, str(temp_parquet_path))

    # Assert that the Parquet file was created
    assert temp_parquet_path.exists()

    # Read the data back from the Parquet file
    try:
        read_df = pd.read_parquet(str(temp_parquet_path))

        # Assert that the data read back is equal to the original DataFrame
        # Use pandas testing utility for DataFrame comparison
        pd.testing.assert_frame_equal(read_df, dummy_df)

    except Exception as e:
        pytest.fail(f"An error occurred during verification: {e}")


def test_load_to_parquet_empty_dataframe(temp_parquet_path):
    """Tests loading an empty DataFrame to Parquet."""
    # Create an empty DataFrame with defined columns
    empty_df = pd.DataFrame(columns=['colA', 'colB', 'colC'])

    # Call the function to load data
    load_to_parquet(empty_df, str(temp_parquet_path))

    # Assert that the Parquet file was created (it should be, even if empty)
    assert temp_parquet_path.exists()

    # Read data back
    try:
        read_df = pd.read_parquet(str(temp_parquet_path))

        # Assert that the DataFrame read back is empty and has the correct columns
        assert read_df.empty
        assert list(read_df.columns) == list(empty_df.columns)

    except Exception as e:
        pytest.fail(f"An error occurred during verification: {e}")


def test_load_to_parquet_directory_creation(tmp_path):
    """Tests that the function creates the directory if it doesn't exist."""
    non_existent_dir = tmp_path / "another_processed_dir"
    parquet_path = non_existent_dir / "test_analytics_dir_create.parquet"

    dummy_df = pd.DataFrame({'id': [99]})

    # Call the function
    load_to_parquet(dummy_df, str(parquet_path))

    # Assert that the directory was created
    assert non_existent_dir.exists()
    assert non_existent_dir.is_dir()

    # Assert that the Parquet file was created inside the directory
    assert parquet_path.exists()

