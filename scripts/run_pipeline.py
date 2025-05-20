# ./scripts/run_pipeline.py

import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import sys
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import logging # Import the logging module

# Set up basic logging for the script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("--- Executing run_pipeline.py (VERSION: DEBUG_LOGS_ENABLED_V15 - .ENV PATHS) ---")

# Load environment variables from .env file
load_dotenv()

# This will come from the .env file, or default to current working directory if not set
PROJECT_ROOT_ENV = os.getenv("PROJECT_ROOT")
if PROJECT_ROOT_ENV is None:
    # Fallback for direct execution without .env or if PROJECT_ROOT is not in .env
    # This assumes the script is run from the project root or its parent directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = Path(script_dir).parent.resolve()
    logging.warning(f"PROJECT_ROOT environment variable not set. Defaulting to: {project_root}")
else:
    project_root = Path(PROJECT_ROOT_ENV).resolve()

# Add the project root to sys.path for module imports
sys.path.insert(0, str(project_root))

# --- DEBUG: Log Current Working Directory and Resolved Project Root ---
logging.debug(f"Current Working Directory: {os.getcwd()}")
logging.debug(f"Resolved Project Root: {project_root}")


# Import Prefect components
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

# Import functions from your src directory
from src.ingestion.google_ads import ingest_google_ads_csv
from src.ingestion.facebook_ads import ingest_facebook_ads_json
from src.ingestion.email_campaigns import ingest_email_campaigns_csv
from src.ingestion.web_traffic import ingest_web_traffic_csv
from src.ingestion.clients import ingest_clients_csv
from src.ingestion.revenue import ingest_revenue_csv

from src.transformation.data_cleaning import clean_marketing_data
from src.transformation.data_joining import join_marketing_data
from src.transformation.metric_calculation import calculate_key_metrics
from src.transformation.attribution import perform_attribution

from src.loading.database_loader import load_to_sqlite
from src.loading.file_loader import load_to_parquet

from src.analytics.report_generator import generate_summary_reports
from src.analytics.lift_analysis import estimate_cross_channel_lift

from src.analytics.dashboard import load_analytics_data


# --- Global Config ---
# Define paths relative to the resolved project_root
RAW_DATA_DIR = project_root / 'data' / 'raw'
PROCESSED_DATA_DIR = project_root / 'data' / 'processed'
REPORTS_DIR = project_root / 'data' / 'reports'
LOAD_FORMAT = 'sqlite'  # or 'parquet'

# File Paths for ingestion (now using absolute paths derived from project_root)
FILE_PATHS = {
    'google_ads': RAW_DATA_DIR / 'google_ads.csv',
    'facebook_ads': RAW_DATA_DIR / 'facebook_ads.json',
    'email_campaigns': RAW_DATA_DIR / 'email_campaigns.csv',
    'web_traffic': RAW_DATA_DIR / 'web_traffic.csv',
    'clients': RAW_DATA_DIR / 'clients.csv',
    'revenue': RAW_DATA_DIR / 'revenue.csv',
}

FINAL_OUTPUT: Dict[str, Dict[str, Optional[str]]] = {
    "parquet": {
        "save": False,
        "path": str(PROCESSED_DATA_DIR / "marketing_data.parquet")
    },
    "sqlite": {
        "save": True,
        "path": str(project_root / "data" / "analytics.db"), # <--- ABSOLUTE PATH
        "table": "marketing_analytics"
    }
}


LIFT_REPORT_PATH = str(REPORTS_DIR / 'cross_channel_lift_report.csv') # <--- ABSOLUTE PATH


# --- Pipeline Tasks ---
@task
def ingest_data_task() -> Optional[Dict[str, pd.DataFrame]]:
    logging.info("Step: Ingestion")
    data = {}
    try:
        for key, file_path_obj in FILE_PATHS.items(): # Iterate over Path objects
            file_path = str(file_path_obj) # Convert Path object to string for os.path.exists
            if not os.path.exists(file_path):
                logging.warning(f"Raw data file not found: {file_path}. Skipping ingestion for {key}.")
                data[key] = pd.DataFrame() # Return empty DataFrame if file not found
            else:
                # Pass string paths to ingestion functions
                if key == 'google_ads': data[key] = ingest_google_ads_csv(file_path)
                elif key == 'facebook_ads': data[key] = ingest_facebook_ads_json(file_path)
                elif key == 'email_campaigns': data[key] = ingest_email_campaigns_csv(file_path)
                elif key == 'web_traffic': data[key] = ingest_web_traffic_csv(file_path)
                elif key == 'clients': data[key] = ingest_clients_csv(file_path)
                elif key == 'revenue': data[key] = ingest_revenue_csv(str(FILE_PATHS['revenue'])) # Ensure revenue path is string
                else: logging.warning(f"Unknown data source key: {key}")

        # Check for None results from ingestion functions
        for key, df in list(data.items()): # Use list to allow modification during iteration
             if df is None:
                  logging.error(f"Ingestion for '{key}' returned None. Removing from dataframes.")
                  del data[key] # Remove None entries

        essential_keys = ['google_ads', 'facebook_ads', 'email_campaigns', 'web_traffic', 'clients']
        if any(key not in data or data[key] is None or (isinstance(data[key], pd.DataFrame) and data[key].empty) for key in essential_keys):
             logging.error("One or more essential sources are empty or missing from dataframes after ingestion. Transformation and subsequent tasks may be affected.")
             pass
        else:
            logging.info("Essential data sources ingested successfully and are not empty.")


        return data
    except FileNotFoundError as e:
        logging.error(f"Missing file during ingestion: {e}")
        raise e # Re-raise the exception
    except Exception as e:
        logging.error(f"Unexpected error during ingestion: {e}")
        raise e # Re-raise the exception


@task
def transform_data_task(dataframes: Optional[Dict[str, pd.DataFrame]]) -> Optional[pd.DataFrame]:
    logging.info("Step: Transformation")
    if not dataframes:
        logging.error("No dataframes provided for transformation.")
        return None

    try:
        logging.info(f"Input dataframes for cleaning: {list(dataframes.keys())}")
        cleaned = clean_marketing_data(dataframes)
        logging.info(f"Dataframes after cleaning: {list(cleaned.keys())}")
        for key, df in cleaned.items():
            logging.info(f"Cleaned '{key}' DataFrame shape: {df.shape if df is not None else 'None'}")
            if df is not None and not df.empty:
                 logging.info(f"Cleaned '{key}' DataFrame columns: {df.columns.tolist()}")
                 logging.info(f"Cleaned '{key}' DataFrame head:\n{df.head()}")


        # Check if essential cleaned dataframes are present and not empty
        essential_cleaned_keys = [k for k in dataframes.keys() if k != 'revenue' and k in cleaned] # Check only keys that were successfully cleaned
        if any(key not in cleaned or cleaned[key] is None or cleaned[key].empty for key in essential_cleaned_keys):
             logging.error("One or more essential cleaned sources are empty or missing. Cannot proceed with joining.")
             return None


        logging.info("Proceeding to joining step.")
        joined = join_marketing_data(cleaned)

        if joined is None or joined.empty:
            logging.error("Joined DataFrame is empty or None. Cannot proceed with metric calculation or attribution.")
            return None
        else:
            logging.info(f"Joined DataFrame shape: {joined.shape}")
            logging.info(f"Joined DataFrame columns: {joined.columns.tolist()}")
            logging.info(f"Joined DataFrame head:\n{joined.head()}")


        logging.info("Proceeding to metric calculation step.")
        metrics = calculate_key_metrics(joined)
        logging.info(f"Metrics DataFrame shape: {metrics.shape}")
        logging.info(f"Metrics DataFrame columns: {metrics.columns.tolist()}")
        logging.info(f"Metrics DataFrame head:\n{metrics.head()}")


        revenue = cleaned.get('revenue') # Get revenue from cleaned dataframes
        if revenue is None or revenue.empty:
             logging.warning("Revenue DataFrame is None or empty. Attribution may not be fully effective.")
        else:
             logging.info(f"Revenue DataFrame shape for attribution: {revenue.shape}")

        logging.info("Proceeding to attribution step.")
        attributed = perform_attribution(metrics, revenue)

        if attributed is None or attributed.empty:
             logging.error("Attributed DataFrame is empty or None.")
             return None
        else:
             logging.info(f"Attributed DataFrame shape: {attributed.shape}")
             logging.info(f"Attributed DataFrame columns: {attributed.columns.tolist()}")
             logging.info(f"Attributed DataFrame head:\n{attributed.head()}")


        logging.info("Data transformation complete.")
        return attributed
    except Exception as e:
        logging.error(f"Unexpected error during transformation: {e}")
        raise e


@task
def load_data_task(df: Optional[pd.DataFrame]) -> Optional[Dict[str, Optional[str]]]:
    logging.info("Step: Loading")

    # --- Add log to check the DataFrame before loading ---
    if df is None:
        logging.warning("Input DataFrame for loading is None. Skipping load.")
        return None
    elif df.empty:
        logging.warning("Input DataFrame for loading is empty. Skipping load.")
        logging.info("Proceeding with loading an empty DataFrame.") # Keep this for now to see if file is created
        return None # Skip loading if DataFrame is empty
    else:
        logging.info(f"Input DataFrame for loading has shape: {df.shape}")
        logging.info(f"Input DataFrame columns: {df.columns.tolist()}")
        logging.info(f"Input DataFrame head:\n{df.head()}")


    output = FINAL_OUTPUT[LOAD_FORMAT]

    try:
        # Ensure the directory for the output file/db exists
        output_dir = os.path.dirname(output['path'])
        if output_dir: # Check if output_dir is not an empty string (e.g., if path is just a filename)
            os.makedirs(output_dir, exist_ok=True)
            logging.info(f"Ensured output directory exists: {output_dir}")


        if LOAD_FORMAT == 'sqlite' and output['save']: # Check save flag
            # Pass table name for sqlite
            load_to_sqlite(df, output['path'], output['table'])
            logging.info(f"Attempted to load data to SQLite: {output['path']}")
            # --- DEBUG: Verify file existence and absolute path after write ---
            db_abs_path = os.path.abspath(output['path'])
            logging.debug(f"SQLite DB expected absolute path: {db_abs_path}")
            logging.debug(f"SQLite DB exists at path: {os.path.exists(db_abs_path)}")

        elif LOAD_FORMAT == 'parquet' and output['save']: # Check save flag
            # table name is None for parquet, load_to_parquet doesn't need it
            load_to_parquet(df, output['path'])
            logging.info(f"Attempted to load data to Parquet: {output['path']}")
            # --- DEBUG: Verify file existence and absolute path after write ---
            parquet_abs_path = os.path.abspath(output['path'])
            logging.debug(f"Parquet file expected absolute path: {parquet_abs_path}")
            logging.debug(f"Parquet file exists at path: {os.path.exists(parquet_abs_path)}")

        else:
             logging.info(f"Loading skipped for format {LOAD_FORMAT} as 'save' is not True.")
             return None # Return None if saving is skipped

        logging.info(f"Data successfully processed for loading to {LOAD_FORMAT} at {output['path']}")
        return output # Return output config on successful processing (even if load_to_sqlite/parquet raises)
    except Exception as e:
        logging.error(f"Error during loading: {e}")
        raise e # Re-raise the exception


@task
def analyze_data_task(output_config: Optional[Dict[str, Optional[str]]]):
    logging.info("Step: Analytics")
    # --- Add log to check output_config ---
    if output_config is None:
        logging.warning("Input output_config for analytics is None. Skipping analytics.")
        return
    else:
        logging.info(f"Input output_config for analytics: {output_config}")
        if output_config.get('path') is None:
             logging.warning("Output config path is None. Skipping analytics.")
             return


    try:
        # Ensure reports directory exists
        os.makedirs(REPORTS_DIR, exist_ok=True) # Use the absolute REPORTS_DIR
        logging.info(f"Ensured reports directory exists: {REPORTS_DIR}")


        final_df = None
        try:
             # Load data from the output path specified in the config
             logging.info(f"Attempting to load final data for analysis from {output_config['path']} (Format: {LOAD_FORMAT})")
             final_df = load_analytics_data(
                 output_config['path'],
                 LOAD_FORMAT,
                 output_config.get('table') # Pass table name if available
             )
             if final_df is not None:
                  logging.info(f"Successfully loaded final data for analysis. Shape: {final_df.shape}")
                  if not final_df.empty:
                       logging.info(f"Final data columns: {final_df.columns.tolist()}")
                       logging.info(f"Final data head:\n{final_df.head()}")
             else:
                  logging.warning("load_analytics_data returned None.")

        except Exception as e:
             logging.error(f"Error reading final data for analysis from {output_config['path']}: {e}")
             return # Stop analysis if data cannot be loaded


        if final_df is None or final_df.empty:
            logging.warning("No final data loaded for analysis or data is empty. Skipping reports and lift analysis.")
            return # Stop analysis if data is empty


        logging.info("Generating summary reports.")
        generate_summary_reports(
            output_config['path'], # Pass the path to the loaded data
            str(REPORTS_DIR), # Pass the absolute REPORTS_DIR as string
            data_format=LOAD_FORMAT,
            table_name=output_config.get('table') # Pass table name only for sqlite
        )
        logging.info("Summary reports generation initiated.")


        logging.info("Estimating cross-channel lift.")
        lift_report_df = estimate_cross_channel_lift(final_df)

        if lift_report_df is not None and not lift_report_df.empty:
            # Ensure the directory for the lift report exists
            lift_report_dir = os.path.dirname(LIFT_REPORT_PATH)
            if lift_report_dir:
                 os.makedirs(lift_report_dir, exist_ok=True)
                 logging.info(f"Ensured directory for lift report exists: {lift_report_dir}")

            lift_report_df.to_csv(LIFT_REPORT_PATH, index=False)
            logging.info(f"Cross-channel lift report generated: {LIFT_REPORT_PATH}")
            # --- DEBUG: Verify file existence and absolute path after write ---
            lift_report_abs_path = os.path.abspath(LIFT_REPORT_PATH)
            logging.debug(f"Lift report expected absolute path: {lift_report_abs_path}")
            logging.debug(f"Lift report exists at path: {os.path.exists(lift_report_abs_path)}")

        else:
            logging.warning("Cross-channel lift analysis did not produce a report.")

    except Exception as e:
        logging.error(f"Error during analytics step: {e}")
        raise e # Re-raise the exception


# --- Prefect Flow Definition ---
@flow(name="Marketing Analytics Pipeline", task_runner=SequentialTaskRunner())
def marketing_pipeline_flow():
    logging.info("Prefect Flow: Marketing pipeline started.")

    ingested_data = ingest_data_task()

    # Check if essential data ingestion failed or resulted in empty data
    essential_keys = ['google_ads', 'facebook_ads', 'email_campaigns', 'web_traffic', 'clients']
    if ingested_data is None or any(key not in ingested_data or ingested_data[key] is None or (isinstance(ingested_data[key], pd.DataFrame) and ingested_data[key].empty) for key in essential_keys):
         logging.error("Essential data ingestion failed or resulted in empty data. Transformation and subsequent tasks will be skipped.")
         transformed_data = None
    else:
         transformed_data = transform_data_task(ingested_data)


    if transformed_data is None or (isinstance(transformed_data, pd.DataFrame) and transformed_data.empty): # Check if transformed_data is None or an empty DataFrame
         logging.error("Transformation task failed or returned None/empty DataFrame. Loading and Analytics tasks will be skipped.")
         output_config = None
    else:
         output_config = load_data_task(transformed_data)


    # The analyze_data_task has its own checks for output_config and data availability
    analyze_data_task(output_config)

    logging.info("Prefect Flow: Marketing pipeline completed.")


# --- Entrypoint to run the Flow ---
if __name__ == '__main__':
    # Ensure data and reports directories exist for local runs
    # These paths are now absolute
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    logging.info(f"Ensured data directories exist: {RAW_DATA_DIR}, {PROCESSED_DATA_DIR}, {REPORTS_DIR}")

    marketing_pipeline_flow()
