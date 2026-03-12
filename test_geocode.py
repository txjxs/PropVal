import pandas as pd
from google.cloud import bigquery
from geopy.geocoders import Nominatim
from dotenv import load_dotenv
import ssl
import geopy.geocoders
import re
import certifi

load_dotenv()

client = bigquery.Client()
query = "SELECT address_full FROM `propval_raw.predictions` LIMIT 5"
df = client.query(query).to_dataframe()

print("RAW BIGQUERY DATA:")
print(df)

# Create an unverified SSL context to bypass the local machine's missing certs
ctx = ssl.create_default_context(cafile=certifi.where())
geopy.geocoders.options.default_ssl_context = ctx

geolocator = Nominatim(user_agent="propval_test_script")
print("\nGEOCODING TESTS:")
for idx, row in df.iterrows():
    address_str = row['address_full']
    
    # 1. Strip out unit/apartment numbers using regex
    # Matches "UNIT 202", "APT 1011", "#414W", "STE B" etc.
    clean_address = re.sub(r'(?i)\b(?:apt|unit|ste|#)\s*\w*', '', address_str).strip()
    # Clean up any double commas or spaces left over
    clean_address = re.sub(r'\s*,\s*,', ',', clean_address)
    clean_address = re.sub(r'\s+', ' ', clean_address)
    
    # 2. Append city/state if missing
    if " VA " not in clean_address and "Virginia" not in clean_address:
        search = f"{clean_address}, Arlington, VA"
    else:
        search = clean_address

    print(f"Original: '{address_str}'")
    print(f"Testing string: '{search}'")
    try:
        loc = geolocator.geocode(search)
        if loc:
            print(f"  SUCCESS! Lat: {loc.latitude}, Lon: {loc.longitude}")
        else:
            print("  FAILED")
    except Exception as e:
        print(f"  ERROR: {e}")
