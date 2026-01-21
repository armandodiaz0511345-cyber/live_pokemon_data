import requests
import json
import time
import pyodbc
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient, ContentSettings
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.core.exceptions import ResourceNotFoundError

# ==========================================
# CONFIGURATION
# ==========================================
API_KEY = "[Your_API_KEY]"
BASE_URL = "https://api.pokemontcg.io/v2/cards"

# AZURE SETTINGS
AZURE_CONNECTION_STRING = "[Your Access Key Connection String]"
CONTAINER_NAME = "pokemon-data"

# SAVE FREQUENCY (To prevent data loss on crash)
PAGES_PER_SAVE = 5  # Uploads to Azure every 5 pages fetched

# ==========================================
# SETUP
# ==========================================
retry_strategy = Retry(
    total=10,
    backoff_factor=2,
    status_forcelist=[404, 429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def get_blob_client(filename):
    """Creates and returns the Azure Blob Client."""
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING,
                                                                   connection_timeout=600,
                                                                   read_timeout=600
                                                                   )
    return blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=filename)

def upload_to_azure(data, filename, verbose=True):
    """Uploads the current list of cards to Azure."""
    try:
        blob_client = get_blob_client(filename)
        json_data = json.dumps(data, ensure_ascii=True, indent=2)
        encoded_data = json_data.encode('utf-8')
        
        my_content_settings = ContentSettings(
            content_type='application/json', 
            content_encoding='utf-8'
        )

        blob_client.upload_blob(
            encoded_data, 
            overwrite=True, 
            content_settings=my_content_settings,
            timeout=600
        )
        if verbose:
            print(f"💾 Checkpoint Saved to Azure: {len(data)} cards total.")
    except Exception as e:
        print(f"❌ WARNING: Failed to save checkpoint to Azure: {e}")

def load_existing_progress(filename):
    """Checks Azure for existing file and loads data to resume."""
    blob_client = get_blob_client(filename)
    try:
        print(f"🔍 Checking Azure for existing file: {filename}...")
        download_stream = blob_client.download_blob()
        data = json.loads(download_stream.readall())
        print(f"✅ Found existing file! Resuming with {len(data)} cards already collected.")
        return data
    except ResourceNotFoundError:
        print("ℹ️ No existing file found. Starting fresh.")
        return []
    except Exception as e:
        print(f"⚠️ Error reading existing blob (starting fresh): {e}")
        return []

# ==========================================
# MAIN LOGIC
# ==========================================

def fetch_all_cards():
    # 1. Generate Filename based on today
    today_str = datetime.now().strftime("%Y%m%d")
    filename = f"pokemon_data_{today_str}.json"

    # 2. Load Progress
    all_cards = load_existing_progress(filename)
    
    batch_size = 250
    # Calculate start page based on how many cards we already have
    # If we have 250 cards, we finished page 1, so we start on page 2.
    start_page = (len(all_cards) // batch_size) + 1
    
    print(f"--- Starting Extraction at Page {start_page} ---")

    page = start_page
    
    while True:
        params = {'page': page, 'pageSize': batch_size}
        headers = {'X-Api-Key': API_KEY}

        try:
            print(f"Requesting Page {page}...", end="")
            
            response = http.get(BASE_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            cards_on_page = data['data']
            
            # If API returns empty list, we are done
            if not cards_on_page:
                print(" Done! (No more data from API)")
                break
                
            all_cards.extend(cards_on_page)
            print(f" Success! Total: {len(all_cards)}")

            # CHECKPOINT: Save to Azure every X pages
            if page % PAGES_PER_SAVE == 0:
                upload_to_azure(all_cards, filename)

            # Check if this was the last page (less than full batch)
            if len(cards_on_page) < batch_size:
                print("--- End of list reached. ---")
                break
            
            page += 1
            time.sleep(0.5) 

        except Exception as e:
            print(f"\n[!] CRITICAL FAIL on Page {page}: {e}")
            print("Attempting emergency save before exiting...")
            upload_to_azure(all_cards, filename)
            break

    # Final Save
    print("--- Extraction Finished. Performing final upload... ---")
    upload_to_azure(all_cards, filename)
    print("DONE.")

# ==========================================
# EXECUTION
# ==========================================
if __name__ == "__main__":
    fetch_all_cards()



# ==========================================
# SQL LOADS
# ==========================================

sql_conn_str = os.getenv("SqlConnectionString")
try:
    with pyodbc.connect(sql_conn_str) as conn:
        with conn.cursor() as cursor:
            logging.info("API Upload finished. Starting SQL Load...")
            cursor.execute("EXEC bronze.load_bronze")
            cursor.execute("EXEC silver.load_silver")
            cursor.execute("EXEC gold.load_gold")
            conn.commit()
            logging.info("SQL Load Successful.")
except Exception as e:
    logging.error(f"SQL Load failed: {e}")