import pandas as pd
from src.transformation.data_cleaning import standardize_column_names, enforce_data_types

def test_standardize_and_enforce_types():
    raw_df = pd.DataFrame({
        'client_id': ['123'],
        'date': ['2024-01-01'],
        'clicks': ['10'],
        'impressions': ['100'],
        'cost_usd': ['5.25'],
        'device_type': ['mobile'],
        'geo': ['US'],
        'campaign_id': ['abc']
    })

    standardized_df = standardize_column_names(raw_df, 'google_ads')
    cleaned_df = enforce_data_types(standardized_df)

    assert cleaned_df['client_id'].dtype == object
    assert pd.api.types.is_datetime64_ns_dtype(cleaned_df['date'])
    assert cleaned_df['clicks'].dtype == 'int64'
    assert cleaned_df['spend_usd'].dtype == 'float64'
    assert cleaned_df['platform'].iloc[0] == 'google_ads'
