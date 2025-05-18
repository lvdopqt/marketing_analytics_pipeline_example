import pandas as pd
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_web_traffic_csv(file_path: str) -> pd.DataFrame | None:
    """
    Ingests web traffic data from a CSV file into a pandas DataFrame.

    Args:
        file_path: The full path to the web_traffic.csv file.

    Returns:
        A pandas DataFrame containing the web traffic data, or None if an error occurs.
    """
    try:
        logging.info(f"Starting ingestion for web traffic data from {file_path}")
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Basic validation: Check if essential columns exist
        required_columns = ['client_id', 'date', 'pageviews', 'sessions', 'bounce_rate', 'avg_session_duration']
        if not all(col in df.columns for col in required_columns):
            logging.error(f"Web Traffic CSV is missing required columns. Found: {df.columns.tolist()}, Required: {required_columns}")
            return None

        # Convert date to datetime objects
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Handle potential parsing errors
        if df['date'].isnull().any():
             logging.warning("Some 'date' values in Web Traffic could not be parsed to datetime.")

        # Ensure numerical columns have appropriate types
        numerical_cols = ['pageviews', 'sessions', 'bounce_rate']
        for col in numerical_cols:
            if col in df.columns:
                 df[col] = pd.to_numeric(df[col], errors='coerce')
                 if df[col].isnull().any():
                      logging.warning(f"Some '{col}' values in Web Traffic could not be parsed to numeric.")

        logging.info(f"Successfully ingested {len(df)} records from {file_path}")
        return df

    except FileNotFoundError:
        logging.error(f"Web Traffic CSV file not found at {file_path}")
        return None
    except pd.errors.EmptyDataError:
        logging.warning(f"Web Traffic CSV file is empty at {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"An error occurred during web traffic CSV ingestion: {e}")
        return None

# Example usage (for testing the function directly)
if __name__ == "__main__":
    # Assuming the script is run from the project root
    web_traffic_file = 'data/raw/web_traffic.csv'
    web_traffic_df = ingest_web_traffic_csv(web_traffic_file)

    if web_traffic_df is not None:
        print("Web Traffic DataFrame Head:")
        print(web_traffic_df.head())
        print("\nWeb Traffic DataFrame Info:")
        web_traffic_df.info()
