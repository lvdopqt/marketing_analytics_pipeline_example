import pandas as pd
import sqlite3
import logging
import os

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str):
    """
    Loads a pandas DataFrame into a SQLite database table.

    Args:
        df: The pandas DataFrame to load.
        db_path: The full path to the SQLite database file (e.g., 'data/analytics.db').
        table_name: The name of the table to load data into.
    """
    logging.info(f"Attempting to load data to SQLite database: {db_path}, table: {table_name}")

    # Ensure the directory for the database file exists
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
            logging.info(f"Ensured directory for database exists: {db_dir}")
        except Exception as e:
            logging.error(f"Failed to create directory {db_dir}: {e}")
            raise e # Re-raise if directory creation is critical for the pipeline

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        logging.info("Successfully connected to SQLite database.")

        logging.info(f"DataFrame shape before to_sql: {df.shape}")
        logging.info(f"DataFrame columns before to_sql: {df.columns.tolist()}")
        logging.info(f"DataFrame head before to_sql:\n{df.head()}")

        df.to_sql(table_name, conn, if_exists='replace', index=False)

        logging.info(f"Successfully loaded {len(df)} records into table '{table_name}' in {db_path}")

    except Exception as e:
        logging.error(f"An error occurred during SQLite data loading: {e}")
        raise e
    finally:
        if conn:
            conn.close()
            logging.info("SQLite database connection closed.")

# Example usage (for testing the function directly)
if __name__ == "__main__":
    dummy_df = pd.DataFrame({
        'client_id': ['C101', 'C102', 'C101'],
        'date': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-02']),
        'platform': ['google_ads', 'facebook_ads', 'google_ads'],
        'spend_usd': [50.5, 40.0, 60.0],
        'clicks': [100, 80, 120]
    })

    test_db_path = 'data/processed/analytics_test.db'
    test_table_name = 'marketing_data'

    os.makedirs(os.path.dirname(test_db_path), exist_ok=True)

    try:
        load_to_sqlite(dummy_df, test_db_path, test_table_name)

        conn_check = sqlite3.connect(test_db_path)
        read_df = pd.read_sql_query(f"SELECT * FROM {test_table_name}", conn_check, parse_dates=['date'])
        logging.info("\n--- Data read back from SQLite ---")
        logging.info(read_df.head())
        logging.info(f"Read back {len(read_df)} records.")
    except Exception as e:
        logging.error(f"\nError in example usage: {e}")
    finally:
        if 'conn_check' in locals() and conn_check:
            conn_check.close()
