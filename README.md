# Marketing Analytics Pipeline

This project implements a simple end-to-end data pipeline for consolidating marketing data from various sources, performing transformations, calculating key metrics, attributing revenue, and generating analytical reports and a dashboard.

## Functionalities

The pipeline performs the following steps:

1.  **Ingestion:** Reads raw marketing data from different sources (Google Ads, Facebook Ads, Email Campaigns, Web Traffic, Clients, Revenue) stored in the `data/raw` directory. It handles different file formats (CSV, JSON) and performs basic initial validation.

2.  **Transformation:**
    * **Cleaning:** Standardizes column names and enforces consistent data types across all sources.
    * **Joining:** Combines data from different marketing platforms and joins it with client information into a single, unified DataFrame.
    * **Metric Calculation:** Computes key marketing metrics (e.g., CTR, CPC, CPM, total interactions) for each record.
    * **Attribution:** Performs multi-touch attribution based on revenue data, adding attributed revenue to the appropriate touchpoints.

3.  **Loading:** Saves the final, transformed, and attributed data to a chosen destination format (SQLite database or Parquet file) in the `data/processed` directory.

4.  **Analytics & Reporting:**
    * Generates various summary reports (CSV files) in the `data/reports` directory based on the loaded data.
    * Performs a simplified cross-channel lift analysis and saves the results to a CSV report.
    * Provides a Streamlit dashboard for interactive exploration and visualization of the final analytics data.

## Reports Generated

The pipeline generates the following reports in the `data/reports` directory:

* `daily_client_spend_report.csv`: Total daily spend aggregated by client.
* `total_clicks_by_platform_report.csv`: Total clicks aggregated by marketing platform.
* `ctr_trends_report.csv`: Aggregate CTR trends over time for relevant platforms.
* `campaign_summary_report.csv`: Aggregated performance metrics (spend, clicks, impressions) by client and campaign.
* `cross_channel_lift_report.csv`: A simplified report comparing channel performance based on attributed revenue and touchpoints.

## Setup

It is highly recommended to use a Python virtual environment to manage project dependencies.

### Creating a Virtual Environment (`venv`)

1.  **Open your terminal or command prompt.**

2.  **Navigate to the project's root directory** (the directory containing this `README.md` file).
    ```bash
    cd path/to/your/marketing_pipeline_project
    ```

3.  **Create the virtual environment:**
    ```bash
    python -m venv .venv
    ```
    This command creates a directory named `.venv` inside your project directory, containing a new Python environment.

4.  **Activate the virtual environment:**
    * On macOS and Linux:
        ```bash
        source .venv/bin/activate
        ```
    * On Windows:
        ```bash
        .venv\Scripts\activate
        ```
    You should see the name of your virtual environment (e.g., `(.venv)`) at the beginning of your terminal prompt, indicating that it's active.

5.  **Install project dependencies:** With the virtual environment activated, install the required libraries using the `requirements.txt` file.
    ```bash
    pip install -r requirements.txt
    ```
    This will install all necessary packages into your isolated virtual environment.

Remember to activate the virtual environment every time you open a new terminal session to work on the project.

## Orchestration with Prefect

This pipeline is orchestrated using [Prefect 2.0](https://docs.prefect.io/). Prefect allows defining pipeline steps as tasks and managing their execution, dependencies, logging, and monitoring.

## How to Run

The project uses a `Makefile` to simplify running common tasks. Ensure you have `make` installed on your system.

1.  **Generating Mock Data**

    To quickly populate the `data/raw` directory with sample data for testing the pipeline, you can use the provided script and Makefile command.

    Run the following command from the project's root directory:

    ```bash
    make generate-data
    ```

    This command executes the `scripts/generate_mock_data.py` script, which will generate CSV and JSON files for clients, ads, email campaigns, web traffic, and revenue in the `data/raw` folder. Running the command multiple times will add more data to the existing files, simulating data arriving over time.
    **NOTE**: There's already some mock data in the project for it to be ready to use

2.  **Activate your virtual environment** (if you haven't already).

3.  **Start the Prefect UI (Orion server):**
    Open a separate terminal, activate your virtual environment, and run:

    ```bash
    make prefect-ui
    ```
    This will start the Prefect server and UI, typically accessible at `http://127.0.0.1:4200`. Keep this terminal open while running the pipeline to monitor its execution in the UI.

4.  **Deploy the Prefect Flow:**
    In another terminal (with the virtual environment activated), run:

    ```bash
    make deploy-prefect
    ```
    This command builds the deployment configuration and registers it with your running Prefect server. You only need to run this again if you make changes to the flow definition (`scripts/run_pipeline.py`).

5.  **Start the Prefect Agent:**
    In a *third* terminal (with the virtual environment activated), run:

    ```bash
    make start-agent
    ```
    This command starts the Prefect agent. The agent connects to the Prefect server and looks for scheduled or triggered flow runs on the 'default' work queue. Keep this terminal open and running while you want your flows to be automatically executed by the Prefect server.

6.  **Run the pipeline (Trigger from UI):**
    After the UI, deployment, and agent are running, go to the Prefect UI (`http://127.0.0.1:4200`), navigate to "Deployments", find your "marketing-pipeline-local" deployment, and click the "Run" button to trigger a new flow run. The agent should then pick up this run and execute the tasks.

7.  **Run the tests:**
    ```bash
    make test
    ```
    This command executes `pytest` to run all tests in the `tests/` directory. Ensure all tests pass before deploying or trusting the pipeline output.

8.  **Run the Streamlit dashboard:**
    ```bash
    make run-dashboard
    ```
    This command starts the Streamlit server for the dashboard. Once started, it will provide a local URL (usually `http://localhost:8501`) that you can open in your web browser to interact with the dashboard.

## Project Structure

```
├── data/                       # Directory for all data files
│   ├── analytics.db            # SQLite database file for the dashboard
│   ├── processed/              # Directory for processed temporary data output (e.g., Parquet files)
│   ├── raw/                    # Directory for raw, source data files
│   │   ├── clients.csv         # Raw client data
│   │   ├── email_campaigns.csv # Raw email campaign data
│   │   ├── facebook_ads.json   # Raw Facebook Ads data
│   │   ├── google_ads.csv      # Raw Google Ads data
│   │   ├── revenue.csv         # Raw revenue data
│   │   └── web_traffic.csv     # Raw web traffic data
│   └── reports/                # Directory for generated analytical reports (CSV files)
│       ├── campaign_summary_report.csv         # Summary report for campaigns
│       ├── cross_channel_lift_report.csv       # Report on cross-channel lift analysis
│       ├── ctr_trends_report.csv               # Report on CTR trends over time
│       ├── daily_client_spend_report.csv       # Report on daily spend per client
│       └── total_clicks_by_platform_report.csv # Report on total clicks per platform
├── Makefile                    # Defines standard commands for project tasks (install, run, deploy, etc.)
├── marketing_pipeline_flow-deployment.yaml # Prefect deployment configuration file
├── README.md                   # Project overview and instructions (this file)
├── requirements.txt            # Lists Python dependencies required for the project
├── scripts/                    # Contains executable Python scripts
│   ├── generate_mock_data.py   # Script to generate dummy data for testing
│   └── run_pipeline.py         # Main script defining and running the Prefect pipeline flow
├── src/                        # Contains the core Python source code modules
│   ├── analytics/              # Modules for data analysis, reporting, and the dashboard
│   │   ├── dashboard.py        # Streamlit script for the interactive dashboard
│   │   ├── lift_analysis.py    # Module for performing cross-channel lift analysis
│   │   └── report_generator.py # Module for generating summary reports
│   ├── config/                 # Configuration settings
│   │   └── settings.py         # Example configuration file
│   ├── ingestion/              # Modules for ingesting data from raw sources
│   │   ├── clients.py          # Ingestion logic for client data
│   │   ├── email_campaigns.py  # Ingestion logic for email campaign data
│   │   ├── facebook_ads.py     # Ingestion logic for Facebook Ads data
│   │   ├── google_ads.py       # Ingestion logic for Google Ads data
│   │   ├── revenue.py          # Ingestion logic for revenue data
│   │   └── web_traffic.py      # Ingestion logic for web traffic data
│   ├── loading/                # Modules for loading processed data to destinations
│   │   ├── database_loader.py  # Loading logic for SQLite database
│   │   └── file_loader.py      # Loading logic for file formats (e.g., Parquet)
│   ├── transformation/         # Modules for data cleaning and transformation
│   │   ├── attribution.py      # Logic for multi-touch revenue attribution
│   │   ├── data_cleaning.py    # Logic for standardizing and cleaning data
│   │   ├── data_joining.py     # Logic for joining dataframes
│   │   └── metric_calculation.py # Logic for calculating key marketing metrics
│   └── utils/                  # Utility functions (if any)
│       └── file_utils.py       # Example utility file
└── tests/                      # Contains test files for the project
```