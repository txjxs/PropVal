"""
PropVal Model Training Script

Trains a Linear Regression model to predict property prices.
Excludes zestimate and rent_estimate to avoid data leakage.
Saves model to GCS for production use.

Usage:
    python src/train.py
"""

import os
import sys
import numpy as np
import pandas as pd
import joblib
from datetime import datetime
from dotenv import load_dotenv

from google.cloud import bigquery, storage
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, r2_score

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', './service-account.json')
GOOGLE_CLOUD_STORAGE_BUCKET_NAME = os.getenv('GOOGLE_CLOUD_STORAGE_BUCKET_NAME')

def query_training_data():
    """Query sold listings from BigQuery"""
    print("Querying training data from BigQuery...")
    
    client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS, project='prop-val')
    
    query = """
    SELECT *
    FROM `prop-val.propval_raw.sold_listing_clean`
    WHERE price > 0 AND sqft > 0
    """
    
    df = client.query(query).to_dataframe()
    print(f"  Loaded {len(df)} properties")
    
    return df

def engineer_features(df):
    """Create features and prepare data for training"""
    print("Engineering features...")
    
    df = df.copy()
    
    # Create price_per_sqft
    df['price_per_sqft'] = df['price'] / df['sqft']
    
    # One-hot encode property_type
    property_dummies = pd.get_dummies(df['property_type'], prefix='type')
    df = pd.concat([df, property_dummies], axis=1)
    
    # One-hot encode zip_code
    zip_dummies = pd.get_dummies(df['zip_code'], prefix='zip')
    df = pd.concat([df, zip_dummies], axis=1)
    
    # Define features (EXCLUDE zestimate and rent_estimate)
    feature_cols = ['bedrooms', 'bathrooms', 'sqft', 'latitude', 'longitude',
                   'days_on_market', 'photo_count', 'has_3d_model', 'has_video',
                   'price_per_sqft'] + list(property_dummies.columns) + list(zip_dummies.columns)
    
    X = df[feature_cols]
    y = df['price']
    
    print(f"  Features: {len(feature_cols)}")
    print(f"  Samples: {len(df)}")
    
    return X, y, feature_cols

def train_model(X, y, feature_cols, df):
    """Train Linear Regression model with cross-validation"""
    print("\nTraining model...")
    
    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Train model
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # Cross-validation
    print("  Running 5-fold cross-validation...")
    cv_scores = cross_val_score(model, X, y, cv=5, 
                                scoring='neg_mean_absolute_percentage_error')
    cv_mape = -cv_scores * 100
    
    # Test set predictions
    predictions = model.predict(X_test)
    
    # Calculate metrics
    rmse = np.sqrt(mean_squared_error(y_test, predictions))
    mape = mean_absolute_percentage_error(y_test, predictions) * 100
    r2 = r2_score(y_test, predictions)
    
    # Residuals
    residuals = y_test - predictions
    mean_residual = residuals.mean()
    median_residual = np.median(residuals)
    
    # Naive baseline
    naive_pred = np.full(len(y_test), y_train.median())
    naive_mape = mean_absolute_percentage_error(y_test, naive_pred) * 100
    
    metrics = {
        'cv_mape_mean': cv_mape.mean(),
        'cv_mape_std': cv_mape.std(),
        'test_rmse': rmse,
        'test_mape': mape,
        'test_r2': r2,
        'mean_residual': mean_residual,
        'median_residual': median_residual,
        'naive_baseline_mape': naive_mape,
        'improvement_vs_baseline': naive_mape - mape
    }
    
    # Print results
    print(f"\n  Cross-Validation MAPE: {cv_mape.mean():.2f}% ± {cv_mape.std():.2f}%")
    print(f"  Test RMSE: ${rmse:,.0f}")
    print(f"  Test MAPE: {mape:.2f}%")
    print(f"  Test R²: {r2:.4f}")
    print(f"  Mean Residual: ${mean_residual:,.0f}")
    print(f"  Improvement vs Baseline: {naive_mape - mape:.2f} percentage points")
    
    # Save metrics to BigQuery for MLOps tracking over time
    log_metrics_to_bq(metrics, len(df))
    
    return model, metrics

def log_metrics_to_bq(metrics, sample_size):
    """Log model performance to BigQuery propval_raw.model_metrics"""
    print("\nLogging metrics to BigQuery...")
    try:
        client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS, project='prop-val')
        table_id = "prop-val.propval_raw.model_metrics"
        
        # Create table if it doesn't exist
        schema = [
            bigquery.SchemaField('training_date', 'TIMESTAMP', mode='REQUIRED'),
            bigquery.SchemaField('model_version', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('test_mape', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('test_rmse', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('test_r2', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('cv_mape_mean', 'FLOAT', mode='REQUIRED'),
            bigquery.SchemaField('training_samples', 'INTEGER', mode='REQUIRED')
        ]
        
        try:
            client.get_table(table_id)
        except Exception:
            # Table does not exist, create it
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)
            print("  Created new table propval_raw.model_metrics")
            
        # Insert Row
        timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        version_str = datetime.now().strftime('%Y%m%d_%H%M')
        
        rows_to_insert = [
            {
                "training_date": timestamp_str,
                "model_version": f"propval_linear_{version_str}",
                "test_mape": float(metrics['test_mape']),
                "test_rmse": float(metrics['test_rmse']),
                "test_r2": float(metrics['test_r2']),
                "cv_mape_mean": float(metrics['cv_mape_mean']),
                "training_samples": int(sample_size)
            }
        ]
        
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors == []:
            print("  Successfully logged metrics to BigQuery.")
        else:
            print(f"  Encountered errors while inserting rows: {errors}")
            
    except Exception as e:
        print(f"  WARNING: Failed to log metrics to BigQuery: {e}")

def save_model(model, feature_cols):
    """Save model locally and upload to GCS"""
    print("\nSaving model...")
    
    # Save locally
    os.makedirs('models', exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    model_path = f'models/propval_linear_{timestamp}.pkl'
    features_path = f'models/feature_names_{timestamp}.pkl'
    
    joblib.dump(model, model_path)
    joblib.dump(feature_cols, features_path)
    
    print(f"  Saved locally: {model_path}")
    
    # Upload to GCS
    if GOOGLE_CLOUD_STORAGE_BUCKET_NAME:
        storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
        bucket = storage_client.bucket(GOOGLE_CLOUD_STORAGE_BUCKET_NAME)
        
        # Upload model
        blob = bucket.blob(f'models/propval_linear_{timestamp}.pkl')
        blob.upload_from_filename(model_path)
        
        # Upload features
        blob = bucket.blob(f'models/feature_names_{timestamp}.pkl')
        blob.upload_from_filename(features_path)
        
        # Also save as "latest" version
        blob = bucket.blob('models/propval_linear_latest.pkl')
        blob.upload_from_filename(model_path)
        
        blob = bucket.blob('models/feature_names_latest.pkl')
        blob.upload_from_filename(features_path)
        
        print(f"  Uploaded to gs://{GOOGLE_CLOUD_STORAGE_BUCKET_NAME}/models/")
        print(f"     - propval_linear_{timestamp}.pkl")
        print(f"     - propval_linear_latest.pkl (production)")
    else:
        print("  WARNING: No GCS bucket configured, skipping upload")

def main():
    """Main training pipeline"""
    print("="*70)
    print("PROPVAL MODEL TRAINING PIPELINE")
    print("="*70)
    
    try:
        df = query_training_data()
        X, y, feature_cols = engineer_features(df)
        model, metrics = train_model(X, y, feature_cols, df)
        save_model(model, feature_cols)
        
        print("\n" + "="*70)
        print("TRAINING COMPLETE")
        print("="*70)
        print(f"  Model MAPE: {metrics['test_mape']:.2f}%")
        print(f"  Model R²: {metrics['test_r2']:.4f}")
        print("="*70)
        
        return 0
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
