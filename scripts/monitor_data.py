# ./scripts/monitor_data.py

import os
import time
import logging
import sys
from pathlib import Path
import asyncio
from dotenv import load_dotenv

# Import watchdog components
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Import Prefect client components for API interaction
from prefect.client import get_client
from prefect.exceptions import PrefectHTTPStatusError, ObjectNotFound

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Determine the actual project root from environment variable
# Fallback to current working directory if not set
PROJECT_ROOT_ENV = os.getenv('PROJECT_ROOT')
if PROJECT_ROOT_ENV is None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = Path(script_dir).parent.resolve()
    logging.warning(f"PROJECT_ROOT environment variable not set. Defaulting to: {project_root}")
else:
    project_root = Path(PROJECT_ROOT_ENV).resolve()

# Add the project root directory to the Python path
sys.path.insert(0, str(project_root))


# --- Configuration ---
# Directory to monitor for new or updated files (now absolute)
MONITORED_DIRECTORY = str(project_root / 'data' / 'raw')
# File types to consider (e.g., CSV, JSON)
FILE_TYPES_TO_MONITOR = ['.csv', '.json']

# Name of the Prefect deployment to trigger (read from environment variable)
# Provide a default for local execution without explicit env var setup
PREFECT_DEPLOYMENT_NAME = os.getenv('PREFECT_DEPLOYMENT_NAME', 'marketing-pipeline-docker')
# Name of the Prefect flow (read from environment variable)
# Provide a default for local execution without explicit env var setup
PREFECT_FLOW_NAME = os.getenv('PREFECT_FLOW_NAME', 'Marketing Analytics Pipeline')


# Retry configuration for triggering the flow via API
MAX_TRIGGER_RETRIES = 25
TRIGGER_RETRY_DELAY_SECONDS = 10


# --- Prefect Flow Trigger Function (Using Prefect API - Trigger by ID) ---
async def trigger_prefect_flow(deployment_name: str, flow_name: str):
    """
    Triggers a Prefect flow run using its deployment ID (obtained by name)
    via the Prefect API client, with retries.

    Args:
        deployment_name: The name of the Prefect deployment.
        flow_name: The name of the Prefect flow (used for logging).
    """
    # Construct the full deployment identifier in the format <FLOW_NAME>/<DEPLOYMENT_NAME>
    full_deployment_identifier = f"{flow_name}/{deployment_name}"
    logging.info(f"Attempting to trigger Prefect flow '{flow_name}' via deployment '{deployment_name}' ({full_deployment_identifier}) using API client.")

    async with get_client() as client:
        deployment_id = None
        logging.info(f"Starting lookup for deployment '{full_deployment_identifier}' using client...")
        for attempt in range(MAX_TRIGGER_RETRIES):
            try:
                logging.info(f"Lookup Attempt {attempt + 1}/{MAX_TRIGGER_RETRIES} for deployment '{full_deployment_identifier}'...")
                deployment = await client.read_deployment_by_name(full_deployment_identifier)
                deployment_id = deployment.id
                logging.info(f"Found deployment '{full_deployment_identifier}' with ID: {deployment_id}")
                break
            except ObjectNotFound:
                 logging.warning(f"Attempt {attempt + 1}/{MAX_TRIGGER_RETRIES}: Deployment '{full_deployment_identifier}' not found yet. It might still be registering.")
                 if attempt < MAX_TRIGGER_RETRIES - 1:
                     logging.info(f"Retrying deployment lookup in {TRIGGER_RETRY_DELAY_SECONDS} seconds...")
                     await asyncio.sleep(TRIGGER_RETRY_DELAY_SECONDS)
                 else:
                     logging.error(f"Max retries ({MAX_TRIGGER_RETRIES}) reached. Deployment '{full_deployment_identifier}' not found after multiple attempts. Cannot trigger flow run.")
                     return
            except PrefectHTTPStatusError as e:
                 logging.error(f"Attempt {attempt + 1}/{MAX_TRIGGER_RETRIES} failed during deployment lookup: Prefect API HTTP Error: {e}")
                 if attempt < MAX_TRIGGER_RETRIES - 1:
                     logging.info(f"Retrying deployment lookup in {TRIGGER_RETRY_DELAY_SECONDS} seconds...")
                     await asyncio.sleep(TRIGGER_RETRY_DELAY_SECONDS)
                 else:
                     logging.error(f"Max retries ({MAX_TRIGGER_RETRIES}) reached. Giving up on deployment lookup.")
                     return
            except Exception as e:
                logging.error(f"Attempt {attempt + 1}/{MAX_TRIGGER_RETRIES} failed during deployment lookup: An unexpected error occurred: {e}")
                if attempt < MAX_TRIGGER_RETRIES - 1:
                    logging.info(f"Retrying deployment lookup in {TRIGGER_RETRY_DELAY_SECONDS} seconds...")
                    await asyncio.sleep(TRIGGER_RETRY_DELAY_SECONDS)
                else:
                    logging.error(f"Max retries ({MAX_TRIGGER_RETRIES}) reached. Giving up on deployment lookup.")
                    return

        if deployment_id:
            logging.info(f"Starting trigger process for deployment ID '{deployment_id}' using client...")
            for attempt in range(MAX_TRIGGER_RETRIES):
                try:
                    logging.info(f"API Trigger Attempt {attempt + 1}/{MAX_TRIGGER_RETRIES} for deployment ID '{deployment_id}'...")
                    flow_run = await client.create_flow_run_from_deployment(deployment_id=deployment_id)
                    logging.info(f"Successfully triggered flow run '{flow_run.name}' ({flow_run.id}) for deployment '{deployment_name}'.")
                    return
                except PrefectHTTPStatusError as e:
                     logging.error(f"Attempt {attempt + 1}/{MAX_TRIGGER_RETRIES} failed during triggering: Prefect API HTTP Error: {e}")
                     if attempt < MAX_TRIGGER_RETRIES - 1:
                         logging.info(f"Retrying triggering in {TRIGGER_RETRY_DELAY_SECONDS} seconds...")
                         await asyncio.sleep(TRIGGER_RETRY_DELAY_SECONDS)
                     else:
                         logging.error(f"Max retries ({MAX_TRIGGER_RETRIES}) reached. Giving up on API triggering deployment '{deployment_name}'.")
                except Exception as e:
                    logging.error(f"Attempt {attempt + 1}/{MAX_TRIGGER_RETRIES} failed during triggering: An unexpected error occurred: {e}")
                    if attempt < MAX_TRIGGER_RETRIES - 1:
                        logging.info(f"Retrying triggering in {TRIGGER_RETRY_DELAY_SECONDS} seconds...")
                        await asyncio.sleep(TRIGGER_RETRY_DELAY_SECONDS)
                    else:
                        logging.error(f"Max retries ({MAX_TRIGGER_RETRIES}) reached. Giving up on API triggering deployment '{deployment_name}'.")
        else:
            logging.error("Could not obtain deployment ID. Skipping trigger attempt.")


# --- Watchdog Event Handler ---
class DataFileHandler(FileSystemEventHandler):
    """
    Custom event handler to react to file system events.
    Triggers the Prefect flow when relevant files are created or modified.
    """
    def on_modified(self, event):
        if not event.is_directory:
            file_path = event.src_path
            if any(file_path.lower().endswith(ext.lower()) for ext in FILE_TYPES_TO_MONITOR):
                logging.info(f"File modified: {file_path}")
                asyncio.run(trigger_prefect_flow(PREFECT_DEPLOYMENT_NAME, PREFECT_FLOW_NAME))

    def on_created(self, event):
        if not event.is_directory:
            file_path = event.src_path
            if any(file_path.lower().endswith(ext.lower()) for ext in FILE_TYPES_TO_MONITOR):
                logging.info(f"File created: {file_path}")
                asyncio.run(trigger_prefect_flow(PREFECT_DEPLOYMENT_NAME, PREFECT_FLOW_NAME))

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info("Starting data directory watchdog monitor...")

    # Ensure the monitored directory exists
    if not os.path.isdir(MONITORED_DIRECTORY):
        logging.error(f"Monitored directory not found: {MONITORED_DIRECTORY}. Please create it.")
        sys.exit(1)

    # Create an event handler instance
    event_handler = DataFileHandler()

    # Create an observer instance
    observer = Observer()

    # Schedule the observer to watch the monitored directory recursively
    observer.schedule(event_handler, MONITORED_DIRECTORY, recursive=True)

    # Start the observer
    observer.start()
    logging.info(f"Watchdog is monitoring directory: {MONITORED_DIRECTORY}")
    logging.info("Press Ctrl+C to stop the monitor.")

    try:
        while observer.is_alive():
            observer.join(1)
    except KeyboardInterrupt:
        logging.info("Monitor stopped manually (KeyboardInterrupt).")
        observer.stop()
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main monitor loop: {e}")
        observer.stop()

    observer.join()
    logging.info("Watchdog monitor finished.")
