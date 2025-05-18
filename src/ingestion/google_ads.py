import pandas as pd
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_google_ads_csv(file_path: str) -> pd.DataFrame | None:
    """
    Ingests Google Ads data from a CSV file into a pandas DataFrame.

    Args:
        file_path: The full path to the google_ads.csv file.

    Returns:
        A pandas DataFrame containing the Google Ads data, or None if an error occurs.
    """
    try:
        logging.info(f"Starting ingestion for Google Ads data from {file_path}")
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Basic validation: Check if essential columns exist
        required_columns = ['campaign_id', 'client_id', 'date', 'clicks', 'impressions', 'cost_usd', 'device_type', 'geo']
        if not all(col in df.columns for col in required_columns):
            logging.error(f"Google Ads CSV is missing required columns. Found: {df.columns.tolist()}, Required: {required_columns}")
            return None

        # Convert date to datetime objects
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Handle potential parsing errors
        if df['date'].isnull().any():
             logging.warning("Some 'date' values in Google Ads could not be parsed to datetime.")

        # Ensure numerical columns have appropriate types
        numerical_cols = ['clicks', 'impressions', 'cost_usd']
        for col in numerical_cols:
            if col in df.columns:
                 df[col] = pd.to_numeric(df[col], errors='coerce')
                 if df[col].isnull().any():
                      logging.warning(f"Some '{col}' values in Google Ads could not be parsed to numeric.")

        logging.info(f"Successfully ingested {len(df)} records from {file_path}")
        return df

    except FileNotFoundError:
        logging.error(f"Google Ads CSV file not found at {file_path}")
        return None
    except pd.errors.EmptyDataError:
        logging.warning(f"Google Ads CSV file is empty at {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"An error occurred during Google Ads CSV ingestion: {e}")
        return None

# Example usage (for testing the function directly)
if __name__ == "__main__":
    # Assuming the script is run from the project root
    google_ads_file = 'data/raw/google_ads.csv'
    google_ads_df = ingest_google_ads_csv(google_ads_file)

    if google_ads_df is not None:
        print("Google Ads DataFrame Head:")
        print(google_ads_df.head())
        print("\nGoogle Ads DataFrame Info:")
        google_ads_df.info()
