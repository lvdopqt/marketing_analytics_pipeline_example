import pandas as pd
import logging
import json

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_facebook_ads_json(file_path: str) -> pd.DataFrame | None:
    """
    Ingests Facebook Ads data from a JSON file into a pandas DataFrame.

    Args:
        file_path: The full path to the facebook_ads.json file.

    Returns:
        A pandas DataFrame containing the Facebook Ads data, or None if an error occurs.
    """
    try:
        logging.info(f"Starting ingestion for Facebook Ads data from {file_path}")
        # Read the JSON file into a DataFrame
        # pd.read_json can often handle a list of JSON objects directly
        df = pd.read_json(file_path)

        # Basic validation: Check if essential columns exist
        # Note: The sample uses 'client' instead of 'client_id', we'll rename this later in transformation
        required_columns = ['fb_campaign_id', 'client', 'date', 'clicks', 'reach', 'spend', 'platform', 'geo']
        if not all(col in df.columns for col in required_columns):
            logging.error(f"Facebook Ads JSON is missing required columns. Found: {df.columns.tolist()}, Required: {required_columns}")
            return None

        # Convert date to datetime objects
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Handle potential parsing errors
        if df['date'].isnull().any():
             logging.warning("Some 'date' values in Facebook Ads could not be parsed to datetime.")

        # Ensure numerical columns have appropriate types
        numerical_cols = ['clicks', 'reach', 'spend']
        for col in numerical_cols:
            if col in df.columns:
                 df[col] = pd.to_numeric(df[col], errors='coerce')
                 if df[col].isnull().any():
                      logging.warning(f"Some '{col}' values in Facebook Ads could not be parsed to numeric.")


        logging.info(f"Successfully ingested {len(df)} records from {file_path}")
        return df

    except FileNotFoundError:
        logging.error(f"Facebook Ads JSON file not found at {file_path}")
        return None
    except pd.errors.EmptyDataError:
         # pd.read_json might not raise EmptyDataError for empty files depending on format
         # You might add a check for empty DataFrame after reading
         logging.warning(f"Facebook Ads JSON file might be empty or malformed at {file_path}")
         # Attempt to read as empty list or handle JSONDecodeError if needed
         try:
             with open(file_path, 'r') as f:
                 data = json.load(f)
                 if not data: # Check if the loaded list/dict is empty
                      logging.warning(f"Facebook Ads JSON file is empty at {file_path}")
                      return pd.DataFrame()
         except json.JSONDecodeError:
              logging.error(f"Facebook Ads JSON file is malformed at {file_path}")
              return None
         except Exception as e:
              logging.error(f"An unexpected error occurred checking empty JSON: {e}")
              return None
         # If we reached here, it wasn't an empty file, but read_json still failed
         logging.error("pd.read_json failed for Facebook Ads JSON, but file was not empty.")
         return None
    except json.JSONDecodeError:
        logging.error(f"Facebook Ads JSON file is malformed and could not be parsed at {file_path}")
        return None
    except Exception as e:
        logging.error(f"An error occurred during Facebook Ads JSON ingestion: {e}")
        return None

# Example usage (for testing the function directly)
if __name__ == "__main__":
    # Assuming the script is run from the project root
    facebook_file = 'data/raw/facebook_ads.json'
    facebook_df = ingest_facebook_ads_json(facebook_file)

    if facebook_df is not None:
        print("Facebook Ads DataFrame Head:")
        print(facebook_df.head())
        print("\nFacebook Ads DataFrame Info:")
        facebook_df.info()
