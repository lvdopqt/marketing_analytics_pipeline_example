import pandas as pd
import logging
from typing import Optional

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ATTRIBUTED_REVENUE_COL = 'attributed_revenue_from_source_usd'
ATTRIBUTED_MODEL_COL = 'attributed_revenue_model_usd'

PLATFORM_TO_CHANNEL_MAP = {
    'google_ads': 'google_ads',
    'facebook_ads': 'facebook',
    'email_campaigns': 'email',
    'web_traffic': 'organic'
}

def _prepare_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    """Add placeholder columns to prevent downstream failures."""
    # Work on a copy to avoid modifying the original DataFrame in place
    processed_df = df.copy()
    if ATTRIBUTED_REVENUE_COL not in processed_df.columns:
        processed_df[ATTRIBUTED_REVENUE_COL] = 0.0
    if ATTRIBUTED_MODEL_COL not in processed_df.columns:
        processed_df[ATTRIBUTED_MODEL_COL] = 0.0
    return processed_df

def _validate_and_prepare_data(joined_df: pd.DataFrame, revenue_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame] | tuple[None, None]:
    """Ensure types and columns are correct before attribution."""
    # Work on copies to avoid modifying original DataFrames
    validated_joined_df = joined_df.copy()
    validated_revenue_df = revenue_df.copy()

    # Ensure join keys are consistent types
    validated_joined_df['client_id'] = validated_joined_df['client_id'].astype(str)
    validated_revenue_df['client_id'] = validated_revenue_df['client_id'].astype(str)

    # Ensure date columns are datetime
    validated_joined_df['date'] = pd.to_datetime(validated_joined_df['date'], errors='coerce')
    validated_revenue_df['date'] = pd.to_datetime(validated_revenue_df['date'], errors='coerce')

    # Check for the required revenue column
    if 'attributed_revenue_usd' not in validated_revenue_df.columns:
        logging.error("Revenue DataFrame is missing 'attributed_revenue_usd' column. Cannot perform join.")
        return None, None # Indicate validation failure

    # Ensure attributed_revenue_usd is numeric
    validated_revenue_df['attributed_revenue_usd'] = pd.to_numeric(validated_revenue_df['attributed_revenue_usd'], errors='coerce').fillna(0.0)


    return validated_joined_df, validated_revenue_df

def _aggregate_revenue(revenue_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate revenue by client, date and channel."""
    daily_channel_revenue = revenue_df.groupby(
        ['client_id', 'date', 'channel']
    )['attributed_revenue_usd'].sum().reset_index()

    return daily_channel_revenue.rename(columns={'attributed_revenue_usd': ATTRIBUTED_REVENUE_COL})

def _map_platforms(joined_df: pd.DataFrame) -> pd.DataFrame:
    """Map platform names to revenue channels for merging."""
    # Work on a copy
    mapped_df = joined_df.copy()
    mapped_df['channel_for_merge'] = mapped_df['platform'].map(PLATFORM_TO_CHANNEL_MAP)
    return mapped_df

def _merge_revenue(joined_df: pd.DataFrame, aggregated_revenue: pd.DataFrame) -> pd.DataFrame:
    """Merge aggregated revenue onto joined marketing data."""
    # Work on a copy
    merged = joined_df.copy()
    merged = pd.merge(
        merged,
        aggregated_revenue,
        left_on=['client_id', 'date', 'channel_for_merge'],
        right_on=['client_id', 'date', 'channel'],
        how='left'
    )
    merged.drop(columns=['channel_for_merge', 'channel'], errors='ignore', inplace=True)

    # Ensure the revenue column exists and fill NaNs with 0.0
    if ATTRIBUTED_REVENUE_COL not in merged.columns:
        logging.warning(f"Column '{ATTRIBUTED_REVENUE_COL}' not found after merge. Adding with 0.0.")
        merged[ATTRIBUTED_REVENUE_COL] = 0.0
    else:
        merged[ATTRIBUTED_REVENUE_COL] = merged[ATTRIBUTED_REVENUE_COL].fillna(0.0)

    # Ensure the model column exists (placeholder)
    if ATTRIBUTED_MODEL_COL not in merged.columns:
        merged[ATTRIBUTED_MODEL_COL] = 0.0

    return merged

def perform_attribution(joined_df: pd.DataFrame, revenue_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Performs rules-based multi-touch attribution modeling based on revenue data.
    """
    logging.info("Starting attribution process")

    # Handle cases where revenue data is explicitly None or empty
    if revenue_df is None or revenue_df.empty:
        logging.warning("No revenue data available. Adding placeholder columns.")
        # Ensure we work on a copy of joined_df
        return _prepare_placeholders(joined_df.copy())

    # Validate and prepare dataframes. This also checks for the required revenue column.
    validated_joined_df, validated_revenue_df = _validate_and_prepare_data(joined_df.copy(), revenue_df.copy())

    # If validation failed (e.g., missing revenue column), _validate_and_prepare_data
    # returns (None, None) and logs an error. We should return a DataFrame with placeholders.
    if validated_joined_df is None or validated_revenue_df is None:
        logging.warning("Validation failed for revenue data. Returning DataFrame with placeholder columns (filled with 0.0).")
        # Return the original joined_df with placeholders added
        return _prepare_placeholders(joined_df.copy())


    # Proceed with attribution if validation passed
    aggregated_revenue = _aggregate_revenue(validated_revenue_df)
    mapped_joined_df = _map_platforms(validated_joined_df)
    result_df = _merge_revenue(mapped_joined_df, aggregated_revenue)

    logging.info(f"Attribution completed. Column '{ATTRIBUTED_REVENUE_COL}' added.")
    return result_df
