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

def save_to_local(data, zip_code, page, mode):
    """RESPONSIBILITY 2: Save to Hard Drive (NDJSON Format)"""
    if not data: return None
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    folder_path = f"data/{mode}/{zip_code}/{date_str}"
    os.makedirs(folder_path, exist_ok=True)
    
    filename = f"{folder_path}/page_{page}.json"
    
    with open(filename, "w") as f:
        for entry in data:
            # Write each object as a single line
            json.dump(entry, f)
            f.write('\n') # Newline character
    
    print(f"Saved locally: {filename}")
    return filename
def upload_to_gcs(local_path, zip_code, page, mode):
    """RESPONSIBILITY 3: Send to Cloud"""
    if not local_path: return
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    # Cloud Path: raw/sold/22202/2023-12-15/page_1.json
    blob_name = f"raw/{mode}/{zip_code}/{date_str}/page_{page}.json"
    
    try:
        storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
        bucket = storage_client.bucket(GOOGLE_CLOUD_STORAGE_BUCKET_NAME)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        print(f"Uploaded to GCS: {blob_name}")
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
        listings = fetch_listings(search_term, page=1, mode="sold")
        local_file = save_to_local(listings, zip_code, page=1, mode="sold")
        if local_file:
            upload_to_gcs(local_file, zip_code, page=1, mode="sold")
        
        # Be polite between zip codes
        time.sleep(2)

if __name__ == "__main__":
    run_pipeline()