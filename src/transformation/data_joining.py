import pandas as pd
import logging

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

EXPECTED_FACT_COLUMNS = [
    'client_id', 'date', 'platform', 'campaign_id', 'device_type', 'geo',
    'clicks', 'impressions', 'spend_usd',
    'emails_sent', 'open_rate', 'click_rate', 'subject_line',
    'pageviews', 'sessions', 'bounce_rate', 'avg_session_duration_seconds',
    'platform_detail'
]

CLIENT_COLUMNS = ['client_id', 'name', 'industry', 'account_manager', 'signup_date']


def _prepare_combined_fact_df(fact_dataframes: dict[str, pd.DataFrame]) -> pd.DataFrame:
    dataframes = [df for df in fact_dataframes.values() if df is not None and not df.empty]
    if not dataframes:
        logging.warning("No non-empty fact DataFrames to combine.")
        # Return an empty DataFrame with all expected columns and appropriate dtypes
        # Define dtypes for the empty DataFrame
        dtype_mapping = {
            'client_id': 'object', 'date': 'datetime64[ns]', 'platform': 'object',
            'campaign_id': 'object', 'device_type': 'object', 'geo': 'object',
            'clicks': 'int', 'impressions': 'int', 'spend_usd': 'float',
            'emails_sent': 'int', 'open_rate': 'float', 'click_rate': 'float',
            'subject_line': 'object', 'pageviews': 'int', 'sessions': 'int',
            'bounce_rate': 'float', 'avg_session_duration_seconds': 'int',
            'platform_detail': 'object'
        }
        # Create empty DataFrame with specified columns and dtypes
        return pd.DataFrame(columns=EXPECTED_FACT_COLUMNS).astype(dtype_mapping)


    df = pd.concat(dataframes, ignore_index=True)

    # Ensure all expected columns are present, adding missing ones with None
    for col in EXPECTED_FACT_COLUMNS:
        if col not in df.columns:
            df[col] = None # Use None or pd.NA

    # Reorder columns for consistency
    existing_cols = [col for col in EXPECTED_FACT_COLUMNS if col in df.columns]
    df = df[existing_cols]

    # Fill missing values and enforce dtypes
    _fill_missing_values(df)

    return df


def _fill_missing_values(df: pd.DataFrame):
    """
    Fills missing values and infers appropriate dtypes for columns after concatenation.
    """
    numeric_cols = [
        'clicks', 'impressions', 'spend_usd', 'emails_sent',
        'pageviews', 'sessions', 'avg_session_duration_seconds'
    ]
    for col in numeric_cols:
        if col in df.columns:
            # FIX: Use infer_objects before fillna to handle potential object dtype and fix FutureWarning
            df[col] = df[col].infer_objects(copy=False).fillna(0)
            # Optionally, convert to integer if appropriate and no NaNs remain
            # Check if the column can be safely converted to integer
            if pd.api.types.is_float_dtype(df[col]) and (df[col] == df[col].astype(int)).all():
                 df[col] = df[col].astype(int)


    rate_cols = ['open_rate', 'click_rate', 'bounce_rate']
    for col in rate_cols:
        if col in df.columns:
            # FIX: Use infer_objects before fillna to handle potential object dtype and fix FutureWarning
            df[col] = df[col].infer_objects(copy=False).fillna(0.0)
            # Ensure rate columns are float
            df[col] = df[col].astype(float)

    object_cols = ['campaign_id', 'device_type', 'geo', 'subject_line', 'platform_detail']
    for col in object_cols:
        if col in df.columns:
            # For object columns, fillna('Unknown') is fine and doesn't need infer_objects
            df[col] = df[col].fillna('Unknown')


def _join_with_clients(fact_df: pd.DataFrame, clients_df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs a left join of the combined fact DataFrame with the clients DataFrame.
    """
    # Ensure join keys have consistent types (client_id should be object/string)
    fact_df['client_id'] = fact_df['client_id'].astype(str)
    clients_df['client_id'] = clients_df['client_id'].astype(str)

    # Perform the merge
    # Use a left join to keep all marketing events and add client info where available
    # If a marketing event has a client_id not in the clients table, client info will be NaN
    merged_df = pd.merge(
        fact_df,
        clients_df[CLIENT_COLUMNS], # Select only necessary client columns
        on='client_id',
        how='left',
        suffixes=('', '_client') # Avoid column name conflicts if any
    )

    # Handle cases where client_id in marketing data is not found in clients table
    # The 'name' column from clients will be NaN for unmatched rows
    if merged_df['name'].isnull().any():
        logging.warning("Some client_ids in marketing data were not found in the clients table after join.")
        # Optionally, fill missing client info columns with placeholders if desired
        client_info_cols = [col for col in CLIENT_COLUMNS if col != 'client_id']
        for col in client_info_cols:
             if col in merged_df.columns:
                  # Fill missing client info with 'Unknown' or similar
                  merged_df[col] = merged_df[col].fillna('Unknown')


    return merged_df


def join_marketing_data(cleaned_dataframes: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
    """
    Main function to join cleaned marketing data DataFrames.
    """
    logging.info("Starting data joining process")

    clients_df = cleaned_dataframes.get('clients')
    if clients_df is None or clients_df.empty:
        logging.error("Clients DataFrame is missing or empty. Cannot perform join.")
        return None

    # Separate fact dataframes and prepare the combined fact table
    fact_dataframes = {k: v for k, v in cleaned_dataframes.items() if k != 'clients'}
    combined_df = _prepare_combined_fact_df(fact_dataframes)

    # If combined fact table is empty, return an empty DataFrame with all final columns
    if combined_df.empty:
        logging.warning("Combined DataFrame is empty. Skipping join with clients.")
        # Define all final columns expected after joining with clients
        all_final_cols = EXPECTED_FACT_COLUMNS + [col for col in CLIENT_COLUMNS if col != 'client_id']
        # Define dtypes for the empty final DataFrame
        dtype_mapping = {
            'client_id': 'object', 'date': 'datetime64[ns]', 'platform': 'object',
            'campaign_id': 'object', 'device_type': 'object', 'geo': 'object',
            'clicks': 'int', 'impressions': 'int', 'spend_usd': 'float',
            'emails_sent': 'int', 'open_rate': 'float', 'click_rate': 'float',
            'subject_line': 'object', 'pageviews': 'int', 'sessions': 'int',
            'bounce_rate': 'float', 'avg_session_duration_seconds': 'int',
            'platform_detail': 'object',
            'name': 'object', 'industry': 'object', 'account_manager': 'object', 'signup_date': 'datetime64[ns]'
        }
        # Create empty DataFrame with specified columns and dtypes
        return pd.DataFrame(columns=all_final_cols).astype(dtype_mapping)


    # Perform the join with the clients DataFrame
    joined_df = _join_with_clients(combined_df, clients_df)

    logging.info(f"Data joining process finished. Final DataFrame has {len(joined_df)} rows.")
    return joined_df
