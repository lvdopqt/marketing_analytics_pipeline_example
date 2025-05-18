import pandas as pd
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_email_campaigns_csv(file_path: str) -> pd.DataFrame | None:
    """
    Ingests email campaign data from a CSV file into a pandas DataFrame.

    Args:
        file_path: The full path to the email_campaigns.csv file.

    Returns:
        A pandas DataFrame containing the email campaign data, or None if an error occurs.
    """
    try:
        logging.info(f"Starting ingestion for email campaigns data from {file_path}")
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Basic validation: Check if essential columns exist
        required_columns = ['email_id', 'client_id', 'date', 'emails_sent', 'open_rate', 'click_rate', 'subject_line']
        if not all(col in df.columns for col in required_columns):
            logging.error(f"Email Campaigns CSV is missing required columns. Found: {df.columns.tolist()}, Required: {required_columns}")
            return None

        # Convert date to datetime objects
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Handle potential parsing errors
        if df['date'].isnull().any():
             logging.warning("Some 'date' values in Email Campaigns could not be parsed to datetime.")

        # Ensure numerical columns have appropriate types (optional but good practice)
        numerical_cols = ['emails_sent', 'open_rate', 'click_rate']
        for col in numerical_cols:
            if col in df.columns:
                 df[col] = pd.to_numeric(df[col], errors='coerce')
                 if df[col].isnull().any():
                      logging.warning(f"Some '{col}' values in Email Campaigns could not be parsed to numeric.")


        logging.info(f"Successfully ingested {len(df)} records from {file_path}")
        return df

    except FileNotFoundError:
        logging.error(f"Email Campaigns CSV file not found at {file_path}")
        return None
    except pd.errors.EmptyDataError:
        logging.warning(f"Email Campaigns CSV file is empty at {file_path}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"An error occurred during email campaigns CSV ingestion: {e}")
        return None

# Example usage (for testing the function directly)
if __name__ == "__main__":
    # Assuming the script is run from the project root
    email_file = 'data/raw/email_campaigns.csv'
    email_df = ingest_email_campaigns_csv(email_file)

    if email_df is not None:
        print("Email Campaigns DataFrame Head:")
        print(email_df.head())
        print("\nEmail Campaigns DataFrame Info:")
        email_df.info()
