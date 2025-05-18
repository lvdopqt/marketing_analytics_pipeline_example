import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REQUIRED_COLS = ['clicks', 'impressions', 'spend_usd', 'emails_sent', 'sessions']


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in REQUIRED_COLS:
        if col not in df.columns:
            logging.warning(f"Column '{col}' missing. Filling with 0.")
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df


def calculate_ctr(df: pd.DataFrame) -> pd.Series:
    df['ctr'] = 0.0
    mask = df['impressions'] > 0
    df.loc[mask, 'ctr'] = (df.loc[mask, 'clicks'] / df.loc[mask, 'impressions']) * 100
    return df['ctr']


def calculate_cpc(df: pd.DataFrame) -> pd.Series:
    df['cpc_usd'] = 0.0
    mask = df['clicks'] > 0
    df.loc[mask, 'cpc_usd'] = df.loc[mask, 'spend_usd'] / df.loc[mask, 'clicks']
    df['cpc_usd'] = df['cpc_usd'].replace([float('inf'), float('-inf')], 0.0)
    return df['cpc_usd']


def calculate_cpm(df: pd.DataFrame) -> pd.Series:
    df['cpm_usd'] = 0.0
    mask = df['impressions'] > 0
    df.loc[mask, 'cpm_usd'] = (df.loc[mask, 'spend_usd'] / df.loc[mask, 'impressions']) * 1000
    df['cpm_usd'] = df['cpm_usd'].replace([float('inf'), float('-inf')], 0.0)
    return df['cpm_usd']


def ensure_rate_columns(df: pd.DataFrame) -> pd.DataFrame:
    for rate in ['open_rate', 'click_rate', 'bounce_rate']:
        if rate not in df.columns:
            df[rate] = 0.0
    return df


def calculate_total_interactions(df: pd.DataFrame) -> pd.Series:
    return df[['clicks', 'emails_sent', 'sessions']].sum(axis=1)


def calculate_key_metrics(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting metric calculation process")
    df = df.copy()

    df = ensure_columns(df)
    df['ctr'] = calculate_ctr(df)
    df['cpc_usd'] = calculate_cpc(df)
    df['cpm_usd'] = calculate_cpm(df)
    df = ensure_rate_columns(df)
    df['total_interactions'] = calculate_total_interactions(df)

    logging.info("Metric calculation process finished")
    return df


def summarize_metrics(df: pd.DataFrame) -> pd.Series:
    summary = {
        'avg_ctr': df['ctr'].mean(),
        'avg_cpc_usd': df['cpc_usd'].mean(),
        'avg_cpm_usd': df['cpm_usd'].mean(),
        'avg_open_rate': df['open_rate'].mean(),
        'avg_click_rate': df['click_rate'].mean(),
        'avg_bounce_rate': df['bounce_rate'].mean(),
        'avg_total_interactions': df['total_interactions'].mean(),
    }
    return pd.Series(summary)
