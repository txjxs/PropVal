# PropVal - Property Valuation Analysis Platform

An end-to-end data engineering and machine learning pipeline for real estate property valuation and investment analysis.

## Project Overview

PropVal automates the collection, processing, and analysis of real estate listing data from Zillow to identify undervalued and overvalued properties. The system combines data engineering best practices with machine learning to provide actionable investment insights.

**Core Objective**: Build an automated system that predicts fair market value for properties and flags potential investment opportunities based on the difference between asking price and predicted value.

## Project Status

All four major phases complete. Production deployment on Google Cloud Platform.

### Completed Phases

**Phase 1: Data Ingestion** - Automated data collection from Zillow API  
**Phase 2: Data Warehousing** - BigQuery external tables for SQL analytics  
**Phase 3: ML Model Training** - Linear regression model for price prediction  
**Phase 4: Automated Inference** - Daily prediction pipeline on Cloud Run  

## Architecture

```
Zillow API
    ↓
[Cloud Run: Data Ingestion]
    ↓
Google Cloud Storage
    ├── raw/sold/          (backup)
    ├── raw/forSale/       (backup)
    ├── clean/sold/        (22 fields, ML-ready)
    └── clean/forSale/     (22 fields, ML-ready)
    ↓
BigQuery External Tables
    ├── sold_listings      (training data)
    └── forsale_listings   (inference data)
    ↓
[ML Training Pipeline]
    ↓
Trained Model → GCS Storage
    ↓
[Cloud Run: Inference]
    ↓
BigQuery Predictions Table
    ├── predicted_price
    ├── asking_price
    ├── valuation_delta
    └── recommendation (UNDERVALUED/FAIR/OVERVALUED)
```

## Technology Stack

**Data Engineering**:
- Python 3.11
- Google Cloud Storage (data lake)
- BigQuery (data warehouse)
- Docker (containerization)
- Cloud Run (serverless compute)

**Machine Learning**:
- Scikit-learn (Linear Regression)
- Pandas (data manipulation)
- NumPy (numerical computing)
- Joblib (model serialization)

**Infrastructure**:
- Google Cloud Platform
- Artifact Registry
- Cloud Run Jobs

## Key Components

### 1. Data Ingestion (`src/ingest.py`)

- Fetches property listings from Zillow API
- Cleans and structures data (22 essential fields)
- Dual storage: raw (backup) + clean (analysis)
- Deployed as Cloud Run jobs (sold + forSale)

**Data Schema** (22 fields):
- Core: property_id, listing_url, price, bedrooms, bathrooms, sqft
- Location: zip_code, city, state, latitude, longitude, address
- Market: zestimate, rent_estimate, days_on_market
- Quality: has_3d_model, has_video, photo_count

### 2. BigQuery External Tables

```sql
propval_raw.sold_listings       -- 366 historical sold properties
propval_raw.forsale_listings    -- 277 current market listings
propval_raw.predictions         -- Daily inference results
```

Enables SQL analytics without data movement from GCS.

### 3. ML Model Training (`src/train.py`)

**Model**: Linear Regression  
**Features**: 23 engineered features (excluding zestimate/rent_estimate)  
**Performance**: 14.36% MAPE, R² 0.9141  
**Validation**: 5-fold cross-validation, residual analysis  

**Training Pipeline**:
1. Query sold listings from BigQuery
2. Feature engineering (price_per_sqft, one-hot encoding)
3. Train Linear Regression model
4. Save model to GCS

### 4. Inference Pipeline (`src/predict.py`)

**Deployed as Cloud Run Job**: Runs on-demand or scheduled

**Process**:
1. Load trained model from GCS
2. Query current for-sale listings from BigQuery
3. Generate price predictions
4. Calculate valuation delta: `(predicted - asking) / predicted`
5. Classify properties:
   - UNDERVALUED: >10% below predicted value
   - FAIR: Within ±10%
   - OVERVALUED: >10% above predicted value
6. Save predictions to BigQuery

**Recent Results**: 277 properties analyzed, 53 undervalued deals identified (19%)

## Project Structure

```
PropVal/
├── src/
│   ├── ingest.py              # Data ingestion pipeline
│   ├── train.py               # Model training script
│   └── predict.py             # Inference pipeline
├── notebooks/
│   └── model_development.ipynb # ML experimentation
├── config/
│   └── cities.json            # Target locations (Arlington, VA)
├── Dockerfile                 # Ingestion job container
├── Dockerfile.inference       # Inference job container
└── requirements.txt           # Python dependencies
```

## Deployment

### Cloud Run Jobs

**propval-ingest-sold**: Collects historical sold property data  
**propval-ingest-forsale**: Collects current market listings  
**propval-predict**: Generates daily price predictions  

### GCS Bucket Structure

```
gs://propval-raw-tn/
├── raw/
│   ├── sold/{zip}/{date}/
│   └── forSale/{zip}/{date}/
├── clean/
│   ├── sold/{zip}/{date}/
│   └── forSale/{zip}/{date}/
└── models/
    ├── propval_linear_latest.pkl
    └── feature_names_latest.pkl
```







## License

MIT License


---

**Last Updated**: December 20, 2024  
**Project Status**: Production Deployment Complete  
**Data Coverage**: 643 total properties (366 sold, 277 for-sale)
