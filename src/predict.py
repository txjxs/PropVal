"""
PropVal Inference Script

Generates price predictions for current for-sale listings.
Loads trained model from GCS and saves predictions to BigQuery.

Usage:
    python src/predict.py
"""

import os
import sys
import numpy as np
import pandas as pd
import joblib
from datetime import datetime
from dotenv import load_dotenv

from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', './service-account.json')
GOOGLE_CLOUD_STORAGE_BUCKET_NAME = os.getenv('GOOGLE_CLOUD_STORAGE_BUCKET_NAME')

def load_model_from_gcs():
    """Load trained model and feature names from GCS"""
    print("Loading model from GCS...")
    
    storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
    bucket = storage_client.bucket(GOOGLE_CLOUD_STORAGE_BUCKET_NAME)
    
    model_blob = bucket.blob('models/propval_linear_latest.pkl')
    features_blob = bucket.blob('models/feature_names_latest.pkl')
    
    model_blob.download_to_filename('temp_model.pkl')
    features_blob.download_to_filename('temp_features.pkl')
    
    model = joblib.load('temp_model.pkl')
    feature_names = joblib.load('temp_features.pkl')
    
    os.remove('temp_model.pkl')
    os.remove('temp_features.pkl')
    
    print(f"  Model loaded: {len(feature_names)} features")
    return model, feature_names

def query_forsale_data():
    """Query for-sale listings from BigQuery"""
    print("Querying for-sale listings from BigQuery...")
    
    client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS, project='prop-val')
    
    query = """
    SELECT *
    FROM `prop-val.propval_raw.forSale_listings_clean`
    WHERE price > 0 AND sqft > 0
    """
    
    df = client.query(query).to_dataframe()
    print(f"  Loaded {len(df)} properties")
    
    return df

def engineer_features(df, feature_names):
    """Apply same feature engineering as training"""
    print("Engineering features...")
    
    df = df.copy()
    
    df['price_per_sqft'] = df['price'] / df['sqft']
    
    property_dummies = pd.get_dummies(df['property_type'], prefix='type')
    df = pd.concat([df, property_dummies], axis=1)
    
    zip_dummies = pd.get_dummies(df['zip_code'], prefix='zip')
    df = pd.concat([df, zip_dummies], axis=1)
    
    for col in feature_names:
        if col not in df.columns:
            df[col] = 0
    
    X = df[feature_names]
    
    print(f"  Features prepared: {len(feature_names)}")
    return X, df

def generate_predictions(model, X, df):
    """Generate predictions and calculate valuation delta"""
    print("\nGenerating predictions...")
    
    predictions = model.predict(X)
    
    results = pd.DataFrame({
        'property_id': df['property_id'],
        'listing_url': df['listing_url'],
        'asking_price': df['price'],
        'predicted_price': predictions.astype(int),
        'address_full': df['address_full'],
        'bedrooms': df['bedrooms'],
        'bathrooms': df['bathrooms'],
        'sqft': df['sqft'],
        'property_type': df['property_type'],
        'zip_code': df['zip_code']
    })
    
    results['valuation_delta'] = results['predicted_price'] - results['asking_price']
    results['valuation_delta_pct'] = (results['valuation_delta'] / results['predicted_price'] * 100).round(2)
    
    def classify_value(pct):
        if pct > 10:
            return 'UNDERVALUED'
        elif pct < -10:
            return 'OVERVALUED'
        else:
            return 'FAIR'
    
    results['recommendation'] = results['valuation_delta_pct'].apply(classify_value)
    results['prediction_date'] = datetime.now().strftime("%Y-%m-%d")
    results['model_version'] = 'linear_v1'
    
    print(f"  Predictions generated: {len(results)}")
    print(f"\n  Recommendations:")
    print(f"    UNDERVALUED: {(results['recommendation'] == 'UNDERVALUED').sum()}")
    print(f"    FAIR: {(results['recommendation'] == 'FAIR').sum()}")
    print(f"    OVERVALUED: {(results['recommendation'] == 'OVERVALUED').sum()}")
    
    return results

def create_predictions_table():
    """Create predictions table in BigQuery if it doesn't exist"""
    print("\nChecking predictions table...")
    
    client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS, project='prop-val')
    
    table_id = 'prop-val.propval_raw.predictions'
    
    schema = [
        bigquery.SchemaField("property_id", "STRING"),
        bigquery.SchemaField("listing_url", "STRING"),
        bigquery.SchemaField("asking_price", "INTEGER"),
        bigquery.SchemaField("predicted_price", "INTEGER"),
        bigquery.SchemaField("valuation_delta", "INTEGER"),
        bigquery.SchemaField("valuation_delta_pct", "FLOAT"),
        bigquery.SchemaField("recommendation", "STRING"),
        bigquery.SchemaField("address_full", "STRING"),
        bigquery.SchemaField("bedrooms", "INTEGER"),
        bigquery.SchemaField("bathrooms", "FLOAT"),
        bigquery.SchemaField("sqft", "INTEGER"),
        bigquery.SchemaField("property_type", "STRING"),
        bigquery.SchemaField("zip_code", "STRING"),
        bigquery.SchemaField("prediction_date", "STRING"),
        bigquery.SchemaField("model_version", "STRING"),
    ]
    
    try:
        client.get_table(table_id)
        print("  Table exists")
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print("  Table created")

def save_predictions(results):
    """Save predictions to BigQuery"""
    print("\nSaving predictions to BigQuery...")
    
    client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS, project='prop-val')
    
    table_id = 'prop-val.propval_raw.predictions'
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    job = client.load_table_from_dataframe(results, table_id, job_config=job_config)
    job.result()
    
    print(f"  Saved {len(results)} predictions to BigQuery")

def main():
    """Main inference pipeline"""
    print("="*70)
    print("PROPVAL INFERENCE PIPELINE")
    print("="*70)
    
    try:
        model, feature_names = load_model_from_gcs()
        df = query_forsale_data()
        X, df_features = engineer_features(df, feature_names)
        results = generate_predictions(model, X, df_features)
        create_predictions_table()
        save_predictions(results)
        
        print("\n" + "="*70)
        print("INFERENCE COMPLETE")
        print("="*70)
        print(f"  Properties analyzed: {len(results)}")
        print(f"  Undervalued deals: {(results['recommendation'] == 'UNDERVALUED').sum()}")
        print("="*70)
        
        return 0
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
