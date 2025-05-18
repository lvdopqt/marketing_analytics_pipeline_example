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
    logging.info(f"Starting data load to SQLite database: {db_path}, table: {table_name}")

    # Ensure the directory for the database file exists
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
        logging.info(f"Created directory for database: {db_dir}")

    try:
        # Connect to the SQLite database. Creates the file if it does not exist.
        conn = sqlite3.connect(db_path)
        logging.info("Successfully connected to SQLite database.")

        # Load the DataFrame into the specified table.
        # Using if_exists='replace' to overwrite the table if it exists.
        # index=False prevents writing the DataFrame index as a column.
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        logging.info(f"Successfully loaded {len(df)} records into table '{table_name}' in {db_path}")

    except Exception as e:
        logging.error(f"An error occurred during SQLite data loading: {e}")
        # Depending on requirements, you might want to re-raise the exception
        # raise e
    finally:
        # Ensure the connection is closed
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("SQLite database connection closed.")

