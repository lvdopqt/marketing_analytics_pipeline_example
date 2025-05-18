import pandas as pd
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_clients_csv(file_path: str) -> pd.DataFrame | None:
    """
    Ingests client data from a CSV file into a pandas DataFrame.

    Args:
        file_path: The full path to the clients.csv file.

    Returns:
        A pandas DataFrame containing the client data, or None if an error occurs.
    """
    try:
        logging.info(f"Starting ingestion for clients data from {file_path}")
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Basic validation: Check if essential columns exist
        required_columns = ['client_id', 'name', 'industry', 'account_manager', 'signup_date']
        if not all(col in df.columns for col in required_columns):
            logging.error(f"Clients CSV is missing required columns. Found: {df.columns.tolist()}, Required: {required_columns}")
            return None

        # Convert signup_date to datetime objects
        df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce')
        # Handle potential parsing errors if 'coerce' was used
        if df['signup_date'].isnull().any():
             logging.warning("Some 'signup_date' values could not be parsed to datetime.")

        logging.info(f"Successfully ingested {len(df)} records from {file_path}")
        return df

    except FileNotFoundError:
        logging.error(f"Clients CSV file not found at {file_path}")
        return None
    except pd.errors.EmptyDataError:
        logging.warning(f"Clients CSV file is empty at {file_path}")
        return pd.DataFrame() # Return empty DataFrame for empty file
    except Exception as e:
        logging.error(f"An error occurred during clients CSV ingestion: {e}")
        return None

# Example usage (for testing the function directly)
if __name__ == "__main__":
    # Assuming the script is run from the project root
    clients_file = 'data/raw/clients.csv'
    clients_df = ingest_clients_csv(clients_file)

    if clients_df is not None:
        print("Clients DataFrame Head:")
        print(clients_df.head())
        print("\nClients DataFrame Info:")
        clients_df.info()
