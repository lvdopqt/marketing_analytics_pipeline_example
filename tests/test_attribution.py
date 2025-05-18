import pandas as pd
import pytest
from src.transformation.attribution import perform_attribution, ATTRIBUTED_REVENUE_COL, ATTRIBUTED_MODEL_COL

@pytest.fixture
def dummy_joined_df():
    return pd.DataFrame({
        'client_id': ['C1', 'C1', 'C2'],
        'date': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'platform': ['google_ads', 'email_campaigns', 'facebook_ads'],
        'clicks': [10, 5, 20],
        'impressions': [100, 50, 200],
        'spend_usd': [25.0, 10.0, 30.0]
    })

@pytest.fixture
def dummy_revenue_df():
    return pd.DataFrame({
        'client_id': ['C1', 'C2'],
        'date': ['2024-01-01', '2024-01-02'],
        'channel': ['google_ads', 'facebook'],
        'attributed_revenue_usd': [100.0, 200.0]
    })

def test_attribution_with_valid_data(dummy_joined_df, dummy_revenue_df):
    result = perform_attribution(dummy_joined_df, dummy_revenue_df)

    assert ATTRIBUTED_REVENUE_COL in result.columns
    assert ATTRIBUTED_MODEL_COL in result.columns
    assert result[ATTRIBUTED_REVENUE_COL].sum() == 300.0
    assert result[result['platform'] == 'email_campaigns'][ATTRIBUTED_REVENUE_COL].iloc[0] == 0.0

def test_attribution_with_no_revenue(dummy_joined_df):
    result = perform_attribution(dummy_joined_df, None)

    assert ATTRIBUTED_REVENUE_COL in result.columns
    assert ATTRIBUTED_MODEL_COL in result.columns
    assert result[ATTRIBUTED_REVENUE_COL].sum() == 0.0

def test_attribution_with_empty_revenue(dummy_joined_df):
    result = perform_attribution(dummy_joined_df, pd.DataFrame())

    assert ATTRIBUTED_REVENUE_COL in result.columns
    assert ATTRIBUTED_MODEL_COL in result.columns
    assert result[ATTRIBUTED_REVENUE_COL].sum() == 0.0

def test_missing_revenue_column_logs_and_defaults(dummy_joined_df):
    broken_revenue_df = pd.DataFrame({
        'client_id': ['C1'],
        'date': ['2024-01-01'],
        'channel': ['google_ads'],
        # missing 'attributed_revenue_usd'
    })
    result = perform_attribution(dummy_joined_df, broken_revenue_df)

    assert ATTRIBUTED_REVENUE_COL in result.columns
    assert result[ATTRIBUTED_REVENUE_COL].sum() == 0.0
