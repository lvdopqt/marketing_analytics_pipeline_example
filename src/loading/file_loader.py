import pandas as pd
import logging
import os

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_to_parquet(df: pd.DataFrame, file_path: str):
    """
    Saves a pandas DataFrame to a Parquet file.

    Args:
        df: The pandas DataFrame to save.
        file_path: The full path to the output Parquet file (e.g., 'data/processed/analytics.parquet').
                   Should end with '.parquet'.
    """
    logging.info(f"Starting data load to Parquet file: {file_path}")

    # Ensure the directory for the file exists
    file_dir = os.path.dirname(file_path)
    if file_dir and not os.path.exists(file_dir):
        os.makedirs(file_dir)
        logging.info(f"Created directory for Parquet file: {file_dir}")

    try:
        # Save the DataFrame to a Parquet file using pyarrow engine.
        # index=False prevents writing the DataFrame index as a column.
        df.to_parquet(file_path, index=False, engine='pyarrow')

        logging.info(f"Successfully saved {len(df)} records to {file_path}")

    except Exception as e:
        logging.error(f"An error occurred during Parquet data loading: {e}")

