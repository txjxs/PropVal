import pandas as pd
import pytest
import sys
import os

# Ensure the src module can be imported
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.train import engineer_features

def test_engineer_features():
    """Test the feature engineering pipeline from train.py"""
    # Create mock dataframe matching expected BigQuery schema
    data = {
        'price': [500000, 750000],
        'sqft': [1000, 2000],
        'property_type': ['SINGLE', 'CONDO'],
        'zip_code': ['22201', '22202'],
        'bedrooms': [2, 3],
        'bathrooms': [1, 2],
        'latitude': [38.8, 38.9],
        'longitude': [-77.1, -77.2],
        'days_on_market': [10, 20],
        'photo_count': [5, 10],
        'has_3d_model': [0, 1],
        'has_video': [1, 0]
    }
    df = pd.DataFrame(data)
    
    # Run the function
    X, y, feature_cols = engineer_features(df)
    
    # Basic assertions
    assert len(X) == 2, "Should return exactly 2 rows"
    assert len(y) == 2, "Should return exactly 2 target values"
    assert list(y) == [500000, 750000], "Target values should match price column"
    
    # Check calculated columns
    assert 'price_per_sqft' in X.columns, "price_per_sqft should be calculated"
    assert X['price_per_sqft'].iloc[0] == 500.0, "price_per_sqft math is incorrect"
    
    # Check one-hot encoding columns
    assert 'type_SINGLE' in X.columns
    assert 'type_CONDO' in X.columns
    assert 'zip_22201' in X.columns
    assert 'zip_22202' in X.columns
    
    # Ensure no data leakage columns are present if they were included
    assert 'zestimate' not in X.columns
    assert 'rent_estimate' not in X.columns
