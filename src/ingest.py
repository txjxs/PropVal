import requests
import json
import os
import time
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv(verbose=True)

API_KEY_ZILLOW = os.getenv('API_KEY_ZILLOW')
GOOGLE_CLOUD_STORAGE_BUCKET_NAME = os.getenv('GOOGLE_CLOUD_STORAGE_BUCKET_NAME')
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
BASE_URL = "https://api.hasdata.com/scrape/zillow/listing"


if not API_KEY_ZILLOW or not GOOGLE_CLOUD_STORAGE_BUCKET_NAME or not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError(".env file is missing secrets!")

def fetch_listings(search_term, page=1, mode="sold"):
    """RESPONSIBILITY 1: Talk to the API"""
    print(f"Fetching '{search_term}' Page {page}...")
    
    headers = {
        "x-api-key": API_KEY_ZILLOW,
        "Content-Type": "application/json"
    }
    
    params = {
        "keyword": search_term, # passing "Arlington, VA 22202"
        "type": mode,
        "page": page
    }
    
    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        if response.status_code == 401:
            print("Error 401: Unauthorized. Check .env")
            return []
        response.raise_for_status()
        return response.json().get("properties", [])
    except Exception as e:
        print(f"API Error: {e}")
        return []


def clean_listing(raw_listing, mode):
    """RESPONSIBILITY 4: Clean the data"""
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    # Helper function to safely get values with defaults
    def safe_get(d, key, default):
        value = d.get(key)
        return value if value is not None else default
    
    # Extract and clean all fields
    cleaned = {
        # Core Features
        'property_id': str(safe_get(raw_listing, 'id', 'UNKNOWN')),
        'listing_url': str(safe_get(raw_listing, 'url', 'UNKNOWN')),
        'price': int(safe_get(raw_listing, 'price', 0)),
        'bedrooms': int(safe_get(raw_listing, 'beds', 0)),
        'bathrooms': float(safe_get(raw_listing, 'baths', 0.0)),
        'sqft': int(safe_get(raw_listing, 'area', 0)),
        'property_type': str(safe_get(raw_listing, 'homeType', 'UNKNOWN')),
        'status': str(safe_get(raw_listing, 'status', 'UNKNOWN')),
        
        # Location Features
        'zip_code': str(safe_get(raw_listing.get('address', {}), 'zipcode', 'UNKNOWN')),
        'city': str(safe_get(raw_listing.get('address', {}), 'city', 'UNKNOWN')),
        'state': str(safe_get(raw_listing.get('address', {}), 'state', 'UNKNOWN')),
        'latitude': float(safe_get(raw_listing, 'latitude', 0.0)),
        'longitude': float(safe_get(raw_listing, 'longitude', 0.0)),
        'address_full': str(safe_get(raw_listing, 'addressRaw', 'UNKNOWN')),
        
        # Market Features
        'zestimate': int(safe_get(raw_listing, 'zestimate', 0)),
        'rent_estimate': int(safe_get(raw_listing, 'rentZestimate', 0)),
        'days_on_market': int(safe_get(raw_listing, 'daysOnZillow', 0)),
        
        # Quality Indicators
        'has_3d_model': bool(safe_get(raw_listing.get('mediaDetails', {}), 'has3DModel', False)),
        'has_video': bool(safe_get(raw_listing.get('mediaDetails', {}), 'hasVideo', False)),
        'photo_count': len(raw_listing.get('photos', [])),
        
        # Metadata
        'fetched_date': date_str,
        'data_source': mode
    }
    
    return cleaned

def save_to_local(data, zip_code, page, mode):
    """RESPONSIBILITY 2: Save to Hard Drive (NDJSON Format) - Both Raw and Clean"""
    if not data: return None, None
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    # Save RAW data
    raw_folder = f"data/raw/{mode}/{zip_code}/{date_str}"
    os.makedirs(raw_folder, exist_ok=True)
    raw_filename = f"{raw_folder}/page_{page}.json"
    
    with open(raw_filename, "w") as f:
        for entry in data:
            json.dump(entry, f)
            f.write('\n')
    
    print(f"Saved raw: {raw_filename}")
    
    # Save CLEAN data
    clean_folder = f"data/clean/{mode}/{zip_code}/{date_str}"
    os.makedirs(clean_folder, exist_ok=True)
    clean_filename = f"{clean_folder}/page_{page}.json"
    
    with open(clean_filename, "w") as f:
        for entry in data:
            cleaned = clean_listing(entry, mode)
            json.dump(cleaned, f)
            f.write('\n')
    
    print(f"Saved clean: {clean_filename}")
    
    return raw_filename, clean_filename

def upload_to_gcs(raw_path, clean_path, zip_code, page, mode):
    """RESPONSIBILITY 3: Send to Cloud - Both Raw and Clean"""
    if not raw_path or not clean_path: return
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    try:
        storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
        bucket = storage_client.bucket(GOOGLE_CLOUD_STORAGE_BUCKET_NAME)
        
        # Upload RAW
        raw_blob_name = f"raw/{mode}/{zip_code}/{date_str}/page_{page}.json"
        raw_blob = bucket.blob(raw_blob_name)
        raw_blob.upload_from_filename(raw_path)
        print(f"Uploaded raw to GCS: {raw_blob_name}")
        
        # Upload CLEAN
        clean_blob_name = f"clean/{mode}/{zip_code}/{date_str}/page_{page}.json"
        clean_blob = bucket.blob(clean_blob_name)
        clean_blob.upload_from_filename(clean_path)
        print(f"Uploaded clean to GCS: {clean_blob_name}")
        
    except Exception as e:
        print(f"Upload Error: {e}")


def run_pipeline():
    """ORCHESTRATOR: Loops through the config file"""
    
    # 1. Load the list of zip codes
    with open("config/cities.json", "r") as f:
        locations = json.load(f)
    
    print(f"Starting Pipeline for {len(locations)} locations...")

    for loc in locations:
        zip_code = loc["zip_code"]
        city = loc["city"]
        state = loc["state"]
        
        # Create the search string: "Arlington, VA 22202"
        search_term = f"{city}, {state} {zip_code}"
        
        # Fetch 1 page per zip (Testing Mode)
        listings = fetch_listings(search_term, page=1, mode="forSale")
        raw_file, clean_file = save_to_local(listings, zip_code, page=1, mode="forSale")
        if raw_file and clean_file:
            upload_to_gcs(raw_file, clean_file, zip_code, page=1, mode="forSale")
        
        # Be polite between zip codes
        time.sleep(2)

if __name__ == "__main__":
    run_pipeline()