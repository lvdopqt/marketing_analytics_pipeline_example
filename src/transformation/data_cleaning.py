import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def standardize_column_names(df: pd.DataFrame, platform_name: str) -> pd.DataFrame:
    logging.info(f"Standardizing column names for {platform_name}")
    cleaned_df = df.copy()

    column_mapping = {
        'google_ads': {
            'campaign_id': 'campaign_id',
            'client_id': 'client_id',
            'date': 'date',
            'clicks': 'clicks',
            'impressions': 'impressions',
            'cost_usd': 'spend_usd',
            'device_type': 'device_type',
            'geo': 'geo'
        },
        'facebook_ads': {
            'fb_campaign_id': 'campaign_id',
            'client': 'client_id',
            'date': 'date',
            'clicks': 'clicks',
            'reach': 'impressions',
            'spend': 'spend_usd',
            'platform': 'platform_detail',
            'geo': 'geo'
        },
        'email_campaigns': {
            'email_id': 'campaign_id',
            'client_id': 'client_id',
            'date': 'date',
            'emails_sent': 'emails_sent',
            'open_rate': 'open_rate',
            'click_rate': 'click_rate',
            'subject_line': 'subject_line'
        },
        'web_traffic': {
            'client_id': 'client_id',
            'date': 'date',
            'pageviews': 'pageviews',
            'sessions': 'sessions',
            'bounce_rate': 'bounce_rate',
            'avg_session_duration': 'avg_session_duration_str'
        },
        'revenue': {
            'client_id': 'client_id',
            'date': 'date',
            'channel': 'channel',
            'attributed_revenue': 'attributed_revenue_usd'
        }
    }

    mapping = column_mapping.get(platform_name)

    if mapping:
        cleaned_df.rename(columns=mapping, inplace=True)
        cleaned_df['platform'] = platform_name
        expected_cols = list(mapping.values()) + ['platform']
        cleaned_df = cleaned_df[[col for col in expected_cols if col in cleaned_df.columns]]
    else:
        logging.warning(f"No column mapping defined for platform: {platform_name}. Returning original DataFrame.")
        cleaned_df['platform'] = platform_name

    return cleaned_df


def enforce_data_types(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Enforcing data types")
    cleaned_df = df.copy()

    dtype_mapping = {
        'client_id': 'object',
        'date': 'datetime64[ns]',
        'clicks': 'int',
        'impressions': 'int',
        'spend_usd': 'float',
        'emails_sent': 'int',
        'open_rate': 'float',
        'click_rate': 'float',
        'pageviews': 'int',
        'sessions': 'int',
        'bounce_rate': 'float',
        'attributed_revenue_usd': 'float'
    }

    for col, dtype in dtype_mapping.items():
        if col in cleaned_df.columns:
            try:
                if dtype == 'datetime64[ns]':
                    cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
                    if cleaned_df[col].isnull().any():
                        logging.warning(f"Coerced some values in '{col}' to NaT.")
                elif dtype in ['int', 'float']:
                    cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')
                    if cleaned_df[col].isnull().any():
                        logging.warning(f"Coerced some values in '{col}' to NaN.")
                    if dtype == 'int' and col in ['clicks', 'impressions', 'emails_sent', 'pageviews', 'sessions']:
                        cleaned_df[col] = cleaned_df[col].fillna(0).astype(int)
                else:
                    cleaned_df[col] = cleaned_df[col].astype(dtype)
            except Exception as e:
                logging.warning(f"Could not enforce type {dtype} for column '{col}': {e}")

    if 'avg_session_duration_str' in cleaned_df.columns:
        def time_string_to_seconds(time_str):
            if pd.isna(time_str):
                return None
            try:
                parts = str(time_str).split(':')
                if len(parts) == 3:
                    h, m, s = map(int, parts)
                    return h * 3600 + m * 60 + s
                elif len(parts) == 2:
                    m, s = map(int, parts)
                    return m * 60 + s
                else:
                    logging.warning(f"Unexpected time string format: {time_str}")
                    return None
            except Exception as e:
                logging.warning(f"Error parsing time string {time_str}: {e}")
                return None

        cleaned_df['avg_session_duration_seconds'] = cleaned_df['avg_session_duration_str'].apply(time_string_to_seconds)
        cleaned_df.drop(columns=['avg_session_duration_str'], inplace=True)
        cleaned_df['avg_session_duration_seconds'] = cleaned_df['avg_session_duration_seconds'].fillna(0).astype(int)

    return cleaned_df


def clean_marketing_data(dataframes: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    logging.info("Starting data cleaning process")
    cleaned_dataframes = {}

    for platform, df in dataframes.items():
        if df is not None and not df.empty:
            df_standardized = standardize_column_names(df, platform)
            df_cleaned = enforce_data_types(df_standardized)
            cleaned_dataframes[platform] = df_cleaned
            logging.info(f"Finished cleaning for platform: {platform}")
        elif df is not None and df.empty:
            logging.warning(f"DataFrame for platform '{platform}' is empty.")
            cleaned_dataframes[platform] = df
        else:
            logging.error(f"DataFrame for platform '{platform}' is None.")

    logging.info("Data cleaning process finished")
    return cleaned_dataframes
