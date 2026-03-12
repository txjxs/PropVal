import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from dotenv import load_dotenv
import ssl
import certifi
import geopy.geocoders
import re
import time
import os

# Load environment variables from .env file for local authentication
load_dotenv()

# Create an unverified SSL context to bypass the local machine's missing certs
ctx = ssl.create_default_context(cafile=certifi.where())
geopy.geocoders.options.default_ssl_context = ctx

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="PropVal - Property Insights Map",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CACHING & DATA RETRIEVAL ---
@st.cache_resource
def get_bq_client():
    """Initializes and caches the BigQuery client."""
    # First, try to load from Streamlit Secrets (for Cloud Deployment)
    if "gcp_service_account" in st.secrets:
        try:
            from google.oauth2 import service_account
            import json
            
            secret_val = st.secrets["gcp_service_account"]
            # Streamlit secrets sometimes parses TOML tables automatically into dicts
            if isinstance(secret_val, str):
                service_account_info = json.loads(secret_val)
            else:
                service_account_info = dict(secret_val)
                
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            return bigquery.Client(credentials=credentials, project=credentials.project_id)
        except Exception as e:
            st.error(f"Error parsing 'gcp_service_account' from Streamlit Secrets: {e}")
            st.stop()
            
    # Fallback to local GOOGLE_APPLICATION_CREDENTIALS environment variable
    try:
        return bigquery.Client()
    except Exception as e:
        st.error(f"Failed to initialize BigQuery client locally. Are your application credentials set? Error: {e}")
        st.stop()

@st.cache_data(ttl=3600)  # Cache data for 1 hour to prevent excessive BQ queries
def load_predictions_data():
    """Loads the predictions data from BigQuery."""
    client = get_bq_client()
    project_id = client.project # Use default project
    
    # Query the predictions table
    query = """
        SELECT 
            property_id,
            prediction_date,
            address_full,
            zip_code,
            bedrooms,
            bathrooms,
            sqft,
            asking_price,
            predicted_price,
            valuation_delta,
            valuation_delta_pct,
            recommendation,
            listing_url
        FROM `propval_analytics.daily_predictions`
        WHERE prediction_date = (SELECT MAX(prediction_date) FROM `propval_analytics.daily_predictions`)
    """
    
    try:
        # If dataset 'propval_analytics' doesn't exist, fallback to 'propval_raw' as mentioned in project architecture
        df = client.query(query).to_dataframe()
    except Exception as e:
        # Fallback query if 'propval_analytics.daily_predictions' isn't available
        st.warning(f"Could not load from 'propval_analytics'. Attempting fallback to 'propval_raw.predictions'. Error details: {e}")
        fallback_query = """
             SELECT 
                property_id,
                DATE(CURRENT_TIMESTAMP()) as prediction_date, -- Approximate if date column missing
                address_full, -- Handling schema variation
                zip_code,
                bedrooms,
                bathrooms,
                sqft,
                asking_price,
                predicted_price,
                valuation_delta,
                recommendation
            FROM `propval_raw.predictions`
        """
        try:
             df = client.query(fallback_query).to_dataframe()
             # Adding generated pct column
             if 'asking_price' in df.columns and 'predicted_price' in df.columns:
                  df['valuation_delta_pct'] = ((df['predicted_price'] - df['asking_price']) / df['predicted_price']) * 100
             df['listing_url'] = "https://www.zillow.com/homedetails/" + df['property_id'].astype(str) + "_zpid/"
                     
        except Exception as fallback_e:
             st.error(f"Failed to load data from BigQuery. Please ensure tables exist. Error: {fallback_e}")
             return pd.DataFrame() # Return empty on failure
         
    return df

@st.cache_data(ttl=60) # Short cache for metrics so retraining changes show up
def load_model_metrics():
    """Loads historical model metrics from BigQuery."""
    client = get_bq_client()
    query = """
        SELECT 
            training_date,
            model_version,
            test_mape,
            test_rmse,
            test_r2,
            training_samples
        FROM `prop-val.propval_raw.model_metrics`
        ORDER BY training_date ASC
    """
    try:
         df = client.query(query).to_dataframe()
         return df
    except Exception as e:
         st.warning(f"Could not load model metrics. Have you trained a model yet? Error: {e}")
         return pd.DataFrame()

# --- GEOCODING FUNCTIONS ---
@st.cache_data(ttl=86400, show_spinner=False)  # Cache geocoding heavily (24 hours) as physical locations rarely change
def geocode_address(address_str, retries=3):
    """Geocodes a single string address using Geopy / Nominatim."""
    if not isinstance(address_str, str) or not address_str.strip():
         return None, None
         
    # 1. Strip out unit/apartment numbers using regex
    # Matches "UNIT 202", "APT 1011", "#414W", "STE B", etc. It's greedy until the comma.
    clean_address = re.sub(r'(?i)\b(?:apt|unit|ste|#)\s*[\w-]*', '', address_str).strip()
    # Clean up any double commas or spaces left over
    clean_address = re.sub(r'\s*,\s*,', ',', clean_address)
    clean_address = re.sub(r'\s+', ' ', clean_address)

    # 2. Append state/country to improve accuracy if missing
    if " VA " not in clean_address and "Virginia" not in clean_address:
        search_address = f"{clean_address}, Arlington, VA"
    else:
        search_address = clean_address

    geolocator = Nominatim(user_agent="propval_dashboard_app")
    
    for attempt in range(retries):
        try:
            location = geolocator.geocode(search_address, timeout=5)
            if location:
                return location.latitude, location.longitude
            else:
                return None, None
        except (GeocoderTimedOut, GeocoderServiceError):
            time.sleep(1) # Backoff
            
    return None, None

def process_coordinates(df):
    """Adds Lat/Lon columns to the DataFrame by geocoding addresses."""
    if df.empty or 'address_full' not in df.columns:
         return df
         
    # Create progress bar container
    progress_text = "Geocoding property addresses... This may take a minute on the first run."
    my_bar = st.progress(0, text=progress_text)
    
    total = len(df)
    lats = []
    lons = []
    
    # Iterate and geocode. Caching happens on individual function call
    for idx, row in df.iterrows():
         lat, lon = geocode_address(row['address_full'])
         lats.append(lat)
         lons.append(lon)
         
         # Update progress bar
         my_bar.progress((idx + 1) / total, text=f"Geocoding: {idx+1}/{total}")
         
    my_bar.empty() # Clear progress bar when done
    
    df['latitude'] = lats
    df['longitude'] = lons
    
    # Drop rows that couldn't be geocoded
    df_clean = df.dropna(subset=['latitude', 'longitude']).copy()
    
    return df_clean


# --- MAIN UI ---
def main():
    st.title("🏡 PropVal Investment Map")
    st.markdown("Interactive dashboard displaying property valuations across Arlington based on daily BigQuery predictions.")
    
    # 1. Load Data
    with st.spinner("Loading predictions from BigQuery..."):
        df_raw = load_predictions_data()
        
    if df_raw.empty:
         st.stop()
         
    # 2. Add Geocoding Data
    df = process_coordinates(df_raw)
    
    if df.empty:
         st.error("No properties could be mapped to coordinate locations.")
         st.stop()

    # --- TABS LAYOUT ---
    tab1, tab2 = st.tabs(["🗺️ Property Map", "📈 Model Performance"])

    with tab1:
        # --- SIDEBAR FILTERS (Only apply to Map Tab but sit on the side) ---
        st.sidebar.header("🕹️ Map Filters")
        
        # Recommendation Filter
        available_recs = sorted(df['recommendation'].unique().tolist())
        # Default to showing UNDERVALUED if present, else show all
        default_rec = ["UNDERVALUED"] if "UNDERVALUED" in available_recs else available_recs
        
        selected_recs = st.sidebar.multiselect(
            "Recommendation Type",
            options=available_recs,
            default=default_rec
        )
        
        # Price Filter
        min_price = int(df['asking_price'].min())
        max_price = int(df['asking_price'].max())
        
        price_range = st.sidebar.slider(
            "Asking Price Range",
            min_value=min_price,
            max_value=max_price,
            value=(min_price, max_price),
            format="$%d"
        )
    
        # Apply Filters
        mask = (
            df['recommendation'].isin(selected_recs) & 
            (df['asking_price'] >= price_range[0]) & 
            (df['asking_price'] <= price_range[1])
        )
        filtered_df = df[mask]

        # --- TOP METRICS ---
        col1, col2, col3, col4 = st.columns(4)
        with col1:
             st.metric("Properties Mapped", len(filtered_df), delta=f"{len(df)} total", delta_color="off")
        with col2:
             # Calculate median delta for filtered properties
             median_val = filtered_df['valuation_delta'].median() if not filtered_df.empty else 0
             st.metric("Median Valuation Delta", f"${median_val:,.0f}")
        with col3:
             undervalued_count = len(filtered_df[filtered_df['recommendation'] == 'UNDERVALUED'])
             st.metric("Undervalued Deals", undervalued_count)
        with col4:
             avg_price = filtered_df['asking_price'].mean() if not filtered_df.empty else 0
             st.metric("Avg Asking Price", f"${avg_price:,.0f}")

        st.divider()

        # --- MAP VISUALIZATION ---
        if not filtered_df.empty:
            # Define color mapping for consistency
            color_discrete_map = {
                 "UNDERVALUED": "#00CC96", # Green
                 "FAIR": "#636EFA",        # Blue
                 "OVERVALUED": "#EF553B"   # Red
            }
            
            # Ensure percentage delta exists for hover formatting
            if 'valuation_delta_pct' not in filtered_df.columns:
                 filtered_df['valuation_delta_pct'] = ((filtered_df['predicted_price'] - filtered_df['asking_price']) / filtered_df['predicted_price']) * 100

            # Create Scatter Mapbox using Plotly
            fig = px.scatter_mapbox(
                filtered_df,
                lat="latitude",
                lon="longitude",
                color="recommendation",
                size_max=15,
                zoom=12,
                hover_name="address_full",
                hover_data={
                    "latitude": False,
                    "longitude": False,
                    "asking_price": ":$,.0f",
                    "predicted_price": ":$,.0f",
                    "valuation_delta": ":$,.0f",
                    "valuation_delta_pct": ":.2f%",
                    "bedrooms": True,
                    "bathrooms": True,
                    "sqft": True
                },
                color_discrete_map=color_discrete_map,
                title="Property Opportunities (Zoom/Pan to explore)"
            )
            
            # Explicitly update traces to show both markers and text
            fig.update_traces(
                mode='markers+text',
                text=filtered_df['recommendation'],
                textposition='top center',
                textfont=dict(
                    size=12,
                    color='black',
                    weight='bold'
                ),
                # Increase marker size slightly so the dot is visible beneath the text
                marker=dict(size=12)
            )
            
            fig.update_layout(
                 mapbox_style="carto-positron", # Clean, light background map
                 margin={"r":0,"t":40,"l":0,"b":0},
                 legend=dict(
                     yanchor="top",
                     y=0.99,
                     xanchor="left",
                     x=0.01
                 )
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No properties match the selected filters.")
            
        # --- DATA TABLE ---
        st.subheader("Data Explorer")
        with st.expander("View Full Data Table"):
             display_cols = ['address_full', 'recommendation', 'asking_price', 'predicted_price', 'valuation_delta', 'bedrooms', 'bathrooms', 'sqft', 'listing_url']
             # Only select columns that exist in the dataframe
             existing_cols = [col for col in display_cols if col in filtered_df.columns]
             
             formatted_df = filtered_df[existing_cols].copy()
             
             # Apply formatting for tabular view
             if 'asking_price' in formatted_df.columns:
                  formatted_df['asking_price'] = formatted_df['asking_price'].apply(lambda x: f"${x:,.0f}")
             if 'predicted_price' in formatted_df.columns:
                  formatted_df['predicted_price'] = formatted_df['predicted_price'].apply(lambda x: f"${x:,.0f}")
             if 'valuation_delta' in formatted_df.columns:
                  formatted_df['valuation_delta'] = formatted_df['valuation_delta'].apply(lambda x: f"${x:,.0f}")
                  
             # Create clickable links using Streamlit's experimental data editor
             st.dataframe(
                  formatted_df,
                  use_container_width=True,
                  column_config={
                       "listing_url": st.column_config.LinkColumn("Zillow Link")
                  },
                  hide_index=True
             )
             
    with tab2:
        st.header("📈 Machine Learning Performance History")
        st.markdown("Track the precision of the Linear Regression model over time as it is retrained on new historical data loops.")
        
        metrics_df = load_model_metrics()
        
        if not metrics_df.empty:
            # Formatting Date
            metrics_df['training_date'] = pd.to_datetime(metrics_df['training_date'])
            # Create two columns for the charts
            m_col1, m_col2 = st.columns(2)
            
            with m_col1:
                # MAPE Chart
                fig_mape = px.line(
                    metrics_df, 
                    x='training_date', 
                    y='test_mape', 
                    markers=True,
                    title="Model Error Rate (MAPE %)",
                    labels={"test_mape": "Mean Absolute Percentage Error", "training_date": "Training Date"}
                )
                fig_mape.update_layout(yaxis_title="Error % (Lower is better)")
                st.plotly_chart(fig_mape, use_container_width=True)
                
            with m_col2:
                # R2 Chart
                fig_r2 = px.line(
                    metrics_df, 
                    x='training_date', 
                    y='test_r2', 
                    markers=True,
                    title="Model Accuracy (R²)",
                    labels={"test_r2": "R-squared Score", "training_date": "Training Date"}
                )
                fig_r2.update_layout(yaxis_title="R² (Higher is better, Max 1.0)")
                st.plotly_chart(fig_r2, use_container_width=True)
                
            # Raw Data View
            st.subheader("Training Run Log")
            st.dataframe(metrics_df, hide_index=True, use_container_width=True)
            
        else:
            st.info("No training records found. Please run the training pipeline first.")


if __name__ == "__main__":
    main()
