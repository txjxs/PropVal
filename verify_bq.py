from google.cloud import bigquery
import os
from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/txjxs/PropVal/service-account.json"
client = bigquery.Client()

def check_table(table_id):
    print(f"\n--- Checking {table_id} ---")
    try:
        query = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
        df = client.query(query).to_dataframe()
        print(f"Total Rows: {df['row_count'].iloc[0]}")
        
        # Try to find a date column to check freshness
        schema = client.get_table(table_id).schema
        date_cols = [f.name for f in schema if f.field_type in ['DATE', 'TIMESTAMP', 'DATETIME']]
        
        if date_cols:
            latest_col = date_cols[0]
            if 'prediction_date' in date_cols:
                latest_col = 'prediction_date'
            elif 'training_date' in date_cols:
                latest_col = 'training_date'
            elif 'date_listed' in date_cols:
                latest_col = 'date_listed'
                
            query_latest = f"SELECT MAX({latest_col}) as latest_date FROM `{table_id}`"
            df_latest = client.query(query_latest).to_dataframe()
            print(f"Latest {latest_col}: {df_latest['latest_date'].iloc[0]}")
            
    except Exception as e:
        print(f"Error checking table: {e}")

check_table("prop-val.propval_raw.forsale_listing")
check_table("prop-val.propval_raw.sold_listing")
check_table("prop-val.propval_raw.predictions")
check_table("prop-val.propval_raw.model_metrics")
