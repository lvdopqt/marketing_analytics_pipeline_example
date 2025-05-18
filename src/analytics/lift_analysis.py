import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CONVERSION_COLUMN = 'attributed_revenue_from_source_usd'

def estimate_cross_channel_lift(df: pd.DataFrame, conversion_column: str = CONVERSION_COLUMN) -> pd.DataFrame | None:
    logging.info(f"Starting cross-channel lift estimation using '{conversion_column}' as conversion indicator.")

    if df is None or df.empty:
        logging.warning("Input DataFrame is empty. Cannot estimate lift.")
        return None

    if conversion_column not in df.columns:
        logging.error(f"Conversion column '{conversion_column}' not found in DataFrame. Cannot estimate lift.")
        return None

    df['is_converted'] = df[conversion_column] > 0

    logging.info("Performing simplified channel performance comparison based on attributed revenue.")

    if 'platform' not in df.columns:
        logging.error("Platform column missing. Cannot perform channel comparison.")
        return None

    channel_performance = df.groupby('platform').agg(
        total_attributed_revenue=(conversion_column, 'sum'),
        total_spend=('spend_usd', 'sum'),
        total_clicks=('clicks', 'sum'),
        total_impressions=('impressions', 'sum'),
        total_touchpoints=('platform', 'count')
    ).reset_index()

    channel_performance['revenue_per_touchpoint'] = 0.0
    mask_touchpoints = channel_performance['total_touchpoints'] > 0
    channel_performance.loc[mask_touchpoints, 'revenue_per_touchpoint'] = (
        channel_performance.loc[mask_touchpoints, 'total_attributed_revenue'] / channel_performance.loc[mask_touchpoints, 'total_touchpoints']
    )

    if 'total_spend' in channel_performance.columns:
        channel_performance['attributed_roi'] = 0.0
        mask_spend = channel_performance['total_spend'] > 0
        channel_performance.loc[mask_spend, 'attributed_roi'] = (
            (channel_performance.loc[mask_spend, 'total_attributed_revenue'] - channel_performance.loc[mask_spend, 'total_spend']) / channel_performance.loc[mask_spend, 'total_spend']
        ) * 100

    channel_performance = channel_performance.sort_values(by='revenue_per_touchpoint', ascending=False)

    logging.info("Simplified channel performance comparison completed.")
    logging.info(f"Resulting DataFrame has {len(channel_performance)} rows.")

    return channel_performance
