# Marketing Analytics Pipeline

This project implements a simple end-to-end data pipeline for consolidating marketing data from various sources, performing transformations, calculating key metrics, attributing revenue, and generating analytical reports and a dashboard.

## Functionalities

The pipeline performs the following steps:

1. **Ingestion:** Reads raw marketing data from different sources (Google Ads, Facebook Ads, Email Campaigns, Web Traffic, Clients, Revenue) stored in the `data/raw` directory. It handles different file formats (CSV, JSON) and performs basic initial validation.

2. **Transformation:**
   * **Cleaning:** Standardizes column names and enforces consistent data types across all sources.
   * **Joining:** Combines data from different marketing platforms and joins it with client information into a single, unified DataFrame.
   * **Metric Calculation:** Computes key marketing metrics (e.g., CTR, CPC, CPM, total interactions) for each record.
   * **Attribution:** Performs multi-touch attribution based on revenue data, adding attributed revenue to the appropriate touchpoints.

3. **Loading:** Saves the final, transformed, and attributed data to a chosen destination format (SQLite database or Parquet file) in the `data/processed` directory.

4. **Analytics & Reporting:**
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

1. **Open your terminal or command prompt.**

2. **Navigate to the project's root directory** (the directory containing this `README.md` file).
   ```bash
   cd path/to/your/marketing_pipeline_project
   ```

3. **Create the virtual environment:**
   ```bash
   python -m venv .venv
   ```
   This command creates a directory named `.venv` inside your project directory, containing a new Python environment.

4. **Activate the virtual environment:**
   * On macOS and Linux:
     ```bash
     source .venv/bin/activate
     ```
   * On Windows:
     ```bash
     .venv\Scripts\activate
     ```
   You should see the name of your virtual environment (e.g., `(.venv)`) at the beginning of your terminal prompt, indicating that it's active.

5. **Install project dependencies:** With the virtual environment activated, install the required libraries using the `requirements.txt` file.
   ```bash
   pip install -r requirements.txt
   ```
   This will install all necessary packages into your isolated virtual environment.

Remember to activate the virtual environment every time you open a new terminal session to work on the project.

## How to Run

The project uses a Makefile to simplify running common tasks. Ensure you have `make` installed on your system.

1. **Generating Mock Data**

    To quickly populate the `data/raw` directory with sample data for testing the pipeline, you can use the provided script and Makefile command.

    Run the following command from the project's root directory:

    ```bash
    make generate-data
    ```

    This command executes the scripts/generate_mock_data.py script, which will generate CSV and JSON files for clients, ads, email campaigns, web traffic, and revenue in the data/raw folder. Running the command multiple times will add more data to the existing files, simulating data arriving over time.
    **NOTE**: There's already some mock data in the project for it to be ready to use


2. **Activate your virtual environment** (if you haven't already).

3. **Run the full pipeline:**
   ```bash
   make run-pipeline
   ```
   This command executes the `run_pipeline.py` script, which performs ingestion, transformation, loading, and analytics steps sequentially.

4. **Run the tests:**
   ```bash
   make test
   ```
   This command executes pytest to run all tests in the `tests/` directory. Ensure all tests pass before deploying or trusting the pipeline output.

5. **Run the Streamlit dashboard:**
   ```bash
   make run-dashboard
   ```
   This command starts the Streamlit server for the dashboard. Once started, it will provide a local URL (usually http://localhost:8501) that you can open in your web browser to interact with the dashboard.

## Project Structure

```
.
├── data/
│   ├── raw/              # Raw data files (CSV, JSON)
│   ├── processed/        # Processed data output (Parquet, SQLite)
│   └── reports/          # Generated reports (CSV)
├── src/
│   ├── ingestion/        # Data ingestion modules
│   ├── transformation/   # Data transformation modules
│   ├── loading/          # Data loading modules
│   └── analytics/        # Data analysis and reporting modules
├── tests/                # Project tests
├── .gitignore            # Files ignored by Git
├── Makefile              # Commands for running tasks
├── requirements.txt      # Project dependencies
└── README.md             # This file
```