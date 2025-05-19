import os
import logging
from datetime import datetime
import pandas as pd
import sys
from typing import Optional, Dict, Any

# Import Prefect components
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

# Add the project root directory to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, project_root)

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


# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Global Config ---
RAW_DATA_DIR = 'data/raw'
PROCESSED_DATA_DIR = 'data/processed'
REPORTS_DIR = 'data/reports'
LOAD_FORMAT = 'sqlite'  # or 'parquet'

# File Paths
FILE_PATHS = {
    'google_ads': os.path.join(RAW_DATA_DIR, 'google_ads.csv'),
    'facebook_ads': os.path.join(RAW_DATA_DIR, 'facebook_ads.json'),
    'email_campaigns': os.path.join(RAW_DATA_DIR, 'email_campaigns.csv'),
    'web_traffic': os.path.join(RAW_DATA_DIR, 'web_traffic.csv'),
    'clients': os.path.join(RAW_DATA_DIR, 'clients.csv'),
    'revenue': os.path.join(RAW_DATA_DIR, 'revenue.csv'),
}

FINAL_OUTPUT: Dict[str, Dict[str, Optional[str]]] = {
    'sqlite': {
        'path': 'data/analytics.db',
        'table': 'marketing_analytics',
    },
    'parquet': {
        'path': os.path.join(PROCESSED_DATA_DIR, 'marketing_analytics.parquet'),
        'table': None,
    }
}

LIFT_REPORT_PATH = os.path.join(REPORTS_DIR, 'cross_channel_lift_report.csv')


# --- Pipeline Tasks ---
@task
def ingest_data_task() -> Optional[Dict[str, pd.DataFrame]]:
    logging.info("Step: Ingestion")
    data = {}
    try:
        for key, file_path in FILE_PATHS.items():
            if not os.path.exists(file_path):
                logging.warning(f"Raw data file not found: {file_path}. Skipping ingestion for {key}.")
                data[key] = pd.DataFrame()
            else:
                if key == 'google_ads': data[key] = ingest_google_ads_csv(file_path)
                elif key == 'facebook_ads': data[key] = ingest_facebook_ads_json(file_path)
                elif key == 'email_campaigns': data[key] = ingest_email_campaigns_csv(file_path)
                elif key == 'web_traffic': data[key] = ingest_web_traffic_csv(file_path)
                elif key == 'clients': data[key] = ingest_clients_csv(file_path)
                elif key == 'revenue': data[key] = ingest_revenue_csv(FILE_PATHS['revenue'])
                else: logging.warning(f"Unknown data source key: {key}")


        essential_keys = ['google_ads', 'facebook_ads', 'email_campaigns', 'web_traffic', 'clients']
        if any(data.get(k) is None or (isinstance(data.get(k), pd.DataFrame) and data.get(k).empty) for k in essential_keys):
             logging.warning("One or more essential sources are empty or missing after ingestion.")
             return data

        return data
    except FileNotFoundError as e:
        logging.error(f"Missing file during ingestion: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error during ingestion: {e}")
        raise e


@task
def transform_data_task(dataframes: Optional[Dict[str, pd.DataFrame]]) -> Optional[pd.DataFrame]:
    logging.info("Step: Transformation")
    if not dataframes:
        logging.error("No dataframes provided for transformation.")
        return None

    try:
        cleaned = clean_marketing_data(dataframes)

        essential_cleaned_keys = [k for k in dataframes.keys() if k != 'revenue']
        if any(cleaned.get(k) is None or (isinstance(cleaned.get(k), pd.DataFrame) and cleaned.get(k).empty) for k in essential_cleaned_keys):
             logging.error("One or more essential cleaned sources are empty or missing.")
             return None

        joined = join_marketing_data(cleaned)

        if joined is None or joined.empty:
            logging.error("Joined DataFrame is empty.")
            expected_joined_cols = joined.columns.tolist() if joined is not None and not joined.empty else []
            return pd.DataFrame(columns=expected_joined_cols)


        metrics = calculate_key_metrics(joined)
        revenue = cleaned.get('revenue')
        attributed = perform_attribution(metrics, revenue)

        return attributed
    except Exception as e:
        logging.error(f"Unexpected error during transformation: {e}")
        raise e


@task
def load_data_task(df: Optional[pd.DataFrame]) -> Optional[Dict[str, Optional[str]]]:
    logging.info("Step: Loading")
    if df is None or df.empty:
        logging.warning("No data to load.")
        return None

    output = FINAL_OUTPUT[LOAD_FORMAT]

    try:
        os.makedirs(os.path.dirname(output['path']), exist_ok=True)
        if LOAD_FORMAT == 'sqlite':
            load_to_sqlite(df, output['path'], output['table'])
        elif LOAD_FORMAT == 'parquet':
            load_to_parquet(df, output['path'])
        logging.info(f"Data successfully loaded to {LOAD_FORMAT}")
        return output
    except Exception as e:
        logging.error(f"Error during loading: {e}")
        raise e


@task
def analyze_data_task(output_config: Optional[Dict[str, Optional[str]]]):
    logging.info("Step: Analytics")
    if not output_config:
        logging.warning("No output config provided for analytics.")
        return

    try:
        os.makedirs(REPORTS_DIR, exist_ok=True)

        final_df = None
        try:
             final_df = load_analytics_data(
                 output_config['path'],
                 LOAD_FORMAT,
                 output_config.get('table')
             )
        except Exception as e:
             logging.error(f"Error reading final data for analysis: {e}")
             return


        if final_df is None or final_df.empty:
            logging.warning("No final data loaded for analysis. Skipping reports and lift analysis.")
            return

        generate_summary_reports(
            output_config['path'],
            REPORTS_DIR,
            data_format=LOAD_FORMAT,
            table_name=output_config['table']
        )
        logging.info("Summary reports generated.")

        lift_report_df = estimate_cross_channel_lift(final_df)

        if lift_report_df is not None and not lift_report_df.empty:
            lift_report_df.to_csv(LIFT_REPORT_PATH, index=False)
            logging.info(f"Cross-channel lift report generated: {LIFT_REPORT_PATH}")
        else:
            logging.warning("Cross-channel lift analysis did not produce a report.")

    except Exception as e:
        logging.error(f"Error during analytics step: {e}")
        raise e


# --- Prefect Flow Definition ---
@flow(name="Marketing Analytics Pipeline", task_runner=SequentialTaskRunner())
def marketing_pipeline_flow():
    logging.info("Prefect Flow: Marketing pipeline started.")

    ingested_data = ingest_data_task()

    essential_keys = ['google_ads', 'facebook_ads', 'email_campaigns', 'web_traffic', 'clients']
    if ingested_data is None or any(ingested_data.get(k) is None or (isinstance(ingested_data.get(k), pd.DataFrame) and ingested_data.get(k).empty) for k in essential_keys):
         logging.error("Essential data ingestion failed or resulted in empty data. Transformation and subsequent tasks will likely be skipped.")
         transformed_data = None
    else:
         transformed_data = transform_data_task(ingested_data)


    if transformed_data is None:
         logging.error("Transformation task failed or returned None. Loading and Analytics tasks will likely be skipped.")
         output_config = None
    else:
         output_config = load_data_task(transformed_data)


    if output_config is None:
         logging.error("Loading task failed or returned None. Analytics task will likely be skipped.")
         pass


    analyze_data_task(output_config)

    logging.info("Prefect Flow: Marketing pipeline completed.")


# --- Entrypoint to run the Flow ---
if __name__ == '__main__':
    marketing_pipeline_flow()
