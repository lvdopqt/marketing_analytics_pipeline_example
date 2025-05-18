import pandas as pd
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_revenue_csv(file_path: str) -> pd.DataFrame | None:
    """
    Ingests revenue attribution data from a CSV file into a pandas DataFrame.
    (Optional bonus data source)

    Args:
        file_path: The full path to the revenue.csv file.

    Returns:
        A pandas DataFrame containing the revenue data, or None if an error occurs.
    """
    try:
        logging.info(f"Starting ingestion for revenue data from {file_path}")
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Basic validation: Check if essential columns exist
        required_columns = ['client_id', 'date', 'channel', 'attributed_revenue']
        if not all(col in df.columns for col in required_columns):
            logging.error(f"Revenue CSV is missing required columns. Found: {df.columns.tolist()}, Required: {required_columns}")
            return None

        # Convert date to datetime objects
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Handle potential parsing errors
        if df['date'].isnull().any():
             logging.warning("Some 'date' values in Revenue could not be parsed to datetime.")

        # Ensure attributed_revenue is numeric
        if 'attributed_revenue' in df.columns:
             df['attributed_revenue'] = pd.to_numeric(df['attributed_revenue'], errors='coerce')
             if df['attributed_revenue'].isnull().any():
                  logging.warning("Some 'attributed_revenue' values could not be parsed to numeric.")

        logging.info(f"Successfully ingested {len(df)} records from {file_path}")
        return df

    except FileNotFoundError:
        logging.error(f"Revenue CSV file not found at {file_path}")
        return None
    except pd.errors.EmptyDataError:
        logging.warning(f"Revenue CSV file is empty at {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"An error occurred during revenue CSV ingestion: {e}")
        return None

# Example usage (for testing the function directly)
if __name__ == "__main__":
    # Assuming the script is run from the project root
    revenue_file = 'data/raw/revenue.csv'
    revenue_df = ingest_revenue_csv(revenue_file)

    if revenue_df is not None:
        print("Revenue DataFrame Head:")
        print(revenue_df.head())
        print("\nRevenue DataFrame Info:")
        revenue_df.info()
