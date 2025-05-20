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

The project now uses Docker for orchestration, making setup and execution simpler.

### Prerequisites

- [Docker](https://www.docker.com/get-started) and Docker Compose installed on your system
- Make (available by default on Linux and macOS, can be installed on Windows)

## How to Run

The project uses a `Makefile` to simplify running common tasks.

1.  **Generating Mock Data**

    To quickly populate the `data/raw` directory with sample data for testing the pipeline, you can use the provided script and Makefile command.

    Run the following command from the project's root directory:

    ```bash
    make generate-data
    ```

    This command executes the `scripts/generate_mock_data.py` script, which will generate CSV and JSON files for clients, ads, email campaigns, web traffic, and revenue in the `data/raw` folder. Running the command multiple times will add more data to the existing files, simulating data arriving over time.
    **NOTE**: There's already some mock data in the project for it to be ready to use

2.  **Start all services:**

    ```bash
    make up
    ```

    This command uses Docker Compose to start:
    - The Prefect UI server
    - A Prefect agent to execute scheduled flows
    - A file watchdog to monitor incoming data files

    All services run in containers, eliminating the need for manual virtual environment setup.

3.  **Stop all services:**

    ```bash
    make down
    ```

    This will stop and remove all the Docker containers started by the `make up` command.

4.  **Run the Streamlit dashboard:**
    ```bash
    make run-dashboard
    ```
    This command starts the Streamlit server for the dashboard. Once started, it will provide a local URL (usually `http://localhost:8501`) that you can open in your web browser to interact with the dashboard.

5.  **Run the tests:** \
    As the tests run locally, you may need to use the virtual environment and install the dependencies to run it. So create it and activate:
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```
    Install the requirements:
    ```bash
    pip install -r requirements.txt
    ```
    And finally run the tests:
    ```bash
    make test
    ```
    This command executes `pytest` to run all tests in the `tests/` directory. Ensure all tests pass before deploying or trusting the pipeline output.

6.  **Clean generated files:**
    ```bash
    make clean
    ```
    This command removes all generated files in the `data/processed/` and `data/reports/` directories, as well as Python cache files.

## Project Structure

```
├── config/                     # Configuration directory
├── data/                       # Directory for all data files
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
├── docker-compose.yml          # Docker Compose configuration for services
├── Dockerfile                  # Dockerfile for building the main application image
├── Dockerfile.agent            # Dockerfile for building the Prefect agent image
├── Makefile                    # Defines standard commands for project tasks (up, down, generate-data, etc.)
├── marketing_pipeline_flow-deployment.yaml # Prefect deployment configuration file
├── README.md                   # Project overview and instructions (this file)
├── requirements.txt            # Lists Python dependencies required for the project
├── scripts/                    # Contains executable Python scripts
│   ├── generate_mock_data.py   # Script to generate dummy data for testing
│   ├── monitor_data.py         # Script to monitor for new data files
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
    ├── test_attribution.py     # Tests for attribution module
    ├── test_dashboard.py       # Tests for dashboard module
    ├── test_data_cleaning.py   # Tests for data cleaning module
    ├── test_data_joining.py    # Tests for data joining module
    ├── test_database_loader.py # Tests for database loader module
    ├── test_file_loader.py     # Tests for file loader module
    ├── test_metric_calculation.py # Tests for metric calculation module
    └── test_report_generator.py   # Tests for report generator module
```