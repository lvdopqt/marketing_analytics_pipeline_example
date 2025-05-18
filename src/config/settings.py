import os

DATA_RAW_DIR = 'data/raw'
DATA_PROCESSED_DIR = 'data/processed'
DATA_REPORTS_DIR = 'data/reports'

GOOGLE_ADS_FILE = os.path.join(DATA_RAW_DIR, 'google_ads.csv')
FACEBOOK_ADS_FILE = os.path.join(DATA_RAW_DIR, 'facebook_ads.json')
EMAIL_CAMPAIGNS_FILE = os.path.join(DATA_RAW_DIR, 'email_campaigns.csv')
WEB_TRAFFIC_FILE = os.path.join(DATA_RAW_DIR, 'web_traffic.csv')
CLIENTS_FILE = os.path.join(DATA_RAW_DIR, 'clients.csv')
REVENUE_FILE = os.path.join(DATA_RAW_DIR, 'revenue.csv')

LOAD_FORMAT = 'sqlite'

if LOAD_FORMAT == 'sqlite':
    FINAL_DATA_PATH = 'data/analytics.db'
    FINAL_TABLE_NAME = 'marketing_analytics'
elif LOAD_FORMAT == 'parquet':
    FINAL_DATA_PATH = os.path.join(DATA_PROCESSED_DIR, 'marketing_analytics.parquet')
    FINAL_TABLE_NAME = None
else:
    raise ValueError(f"Invalid load format: {LOAD_FORMAT}")
