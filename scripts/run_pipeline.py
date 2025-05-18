# ./run_pipeline.py

import os
import logging
from datetime import datetime

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

FINAL_OUTPUT = {
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

# --- Pipeline Steps ---
def ingest_data():
    logging.info("Step: Ingestion")
    data = {}
    try:
        data['google_ads'] = ingest_google_ads_csv(FILE_PATHS['google_ads'])
        data['facebook_ads'] = ingest_facebook_ads_json(FILE_PATHS['facebook_ads'])
        data['email_campaigns'] = ingest_email_campaigns_csv(FILE_PATHS['email_campaigns'])
        data['web_traffic'] = ingest_web_traffic_csv(FILE_PATHS['web_traffic'])
        data['clients'] = ingest_clients_csv(FILE_PATHS['clients'])
        data['revenue'] = ingest_revenue_csv(FILE_PATHS['revenue'])

        # Validate essential inputs
        essential_keys = ['google_ads', 'facebook_ads', 'email_campaigns', 'web_traffic', 'clients']
        if any(data[k] is None or data[k].empty for k in essential_keys):
            logging.warning("One or more essential sources are empty or missing.")
            # Optional: return None here to abort
        return data
    except FileNotFoundError as e:
        logging.error(f"Missing file during ingestion: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during ingestion: {e}")
    return None


def transform_data(dataframes):
    logging.info("Step: Transformation")
    try:
        cleaned = clean_marketing_data(dataframes)
        joined = join_marketing_data(cleaned)

        if joined is None or joined.empty:
            logging.error("Joined DataFrame is empty.")
            return None

        metrics = calculate_key_metrics(joined)
        revenue = cleaned.get('revenue')
        attributed = perform_attribution(metrics, revenue)

        return attributed
    except Exception as e:
        logging.error(f"Unexpected error during transformation: {e}")
        return None


def load_data(df):
    logging.info("Step: Loading")
    output = FINAL_OUTPUT[LOAD_FORMAT]

    try:
        os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
        if LOAD_FORMAT == 'sqlite':
            load_to_sqlite(df, output['path'], output['table'])
        elif LOAD_FORMAT == 'parquet':
            load_to_parquet(df, output['path'])
        logging.info(f"Data successfully loaded to {LOAD_FORMAT}")
        return output
    except Exception as e:
        logging.error(f"Error during loading: {e}")
        return None


def analyze_data(output_config):
    logging.info("Step: Analytics")
    if not output_config:
        logging.warning("No output config provided for analytics.")
        return

    try:
        os.makedirs(REPORTS_DIR, exist_ok=True)

        # Load the final processed data to perform analysis on
        final_df = None
        try:
             if output_config['path'].endswith('.db'):
                  # Reuse dashboard's loading logic if available, or implement simple read
                  from src.analytics.dashboard import load_analytics_data # Assuming this function exists and works
                  final_df = load_analytics_data(
                      output_config['path'],
                      LOAD_FORMAT,
                      output_config.get('table')
                  )
             elif output_config['path'].endswith('.parquet'):
                  final_df = pd.read_parquet(output_config['path'])
             else:
                  logging.error(f"Unsupported output format for analysis: {output_config['path']}")
                  return # Exit if format is unsupported

        except Exception as e:
             logging.error(f"Error reading final data for analysis: {e}")
             final_df = None # Ensure final_df is None on error


        if final_df is None or final_df.empty:
            logging.warning("No final data loaded for analysis. Skipping reports and lift analysis.")
            return

        # Generate summary reports
        generate_summary_reports(
            output_config['path'],
            REPORTS_DIR,
            data_format=LOAD_FORMAT,
            table_name=output_config['table']
        )
        logging.info("Summary reports generated.")

        # Perform cross-channel lift analysis
        lift_report_df = estimate_cross_channel_lift(final_df)

        if lift_report_df is not None and not lift_report_df.empty:
            # Save the lift analysis report
            lift_report_df.to_csv(LIFT_REPORT_PATH, index=False)
            logging.info(f"Cross-channel lift report generated: {LIFT_REPORT_PATH}")
        else:
            logging.warning("Cross-channel lift analysis did not produce a report.")

    except Exception as e:
        logging.error(f"Error during analytics step: {e}")


def run_pipeline():
    logging.info("Marketing pipeline started.")
    data = ingest_data()
    if not data:
        logging.error("Ingestion failed. Aborting pipeline.")
        return

    transformed = transform_data(data)
    if transformed is None:
        logging.error("Transformation failed. Aborting pipeline.")
        return

    output_config = load_data(transformed)
    if not output_config:
        logging.error("Loading failed. Aborting pipeline.")
        return

    analyze_data(output_config)
    logging.info("Marketing pipeline completed successfully.")


# --- Entrypoint ---
if __name__ == '__main__':
    run_pipeline()