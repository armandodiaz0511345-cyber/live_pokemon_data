import azure.functions as func
import logging
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

# REQUIRED: The file must be named 'function_app.py' for Azure to find it.
app = func.FunctionApp()

# ==========================================
# TRIGGER CONFIGURATION
# ==========================================
# We renamed the function below to "pokemon_data_pipeline_daily" as requested.
# Schedule: "0 0 0 * * *" = Daily at Midnight.
@app.schedule(schedule="0 0 2 * * *", arg_name="mytimer", run_on_startup=True)
def pokemon_data_pipeline_daily(mytimer: func.TimerRequest) -> None:
    logging.info('--- Starting Pokemon Data Pipeline ---')
    
    # Step 1: Run the Extraction Logic
    success = fetch_all_cards()

    # Step 2: Run SQL Load ONLY if extraction was successful
    if success:
        # --- NEW WAITING WINDOW ---
        logging.info("Extraction and Blob upload complete. Waiting 15 seconds for Azure Storage consistency...")
        time.sleep(15) 
        # --------------------------
        run_sql_load()
    else:
        logging.error("Skipping SQL load due to extraction failure.")

# ==========================================
# CORE LOGIC
# ==========================================

def fetch_all_cards():
    # CONFIGURATION (Loaded from Environment Variables)
    # Ideally, move API_KEY to env vars too: os.getenv("PokemonApiKey")
    API_KEY = "[Your API Key]" 
    BASE_URL = "https://api.pokemontcg.io/v2/cards"
    
    # Connection string from Function App Settings
    AZURE_CONNECTION_STRING = os.getenv("MainDataStorage") 
    CONTAINER_NAME = "pokemon-data"
    PAGES_PER_SAVE = 5

    # SETUP HTTP SESSION
    retry_strategy = Retry(
        total=10,
        backoff_factor=2,
        status_forcelist=[404, 429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    # HELPER: Get Blob Client
    def get_blob_client(filename):
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING,
                                                                       connection_timeout=600,
                                                                       read_timeout=600
                                                                       )
        return blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=filename)

    # HELPER: Upload to Azure
    def upload_to_azure(data, filename, verbose=True):
        try:
            blob_client = get_blob_client(filename)
            json_data = json.dumps(data, ensure_ascii=True, indent=2)
            encoded_data = json_data.encode('utf-8')
            
            my_content_settings = ContentSettings(content_type='application/json', content_encoding='utf-8')

            blob_client.upload_blob(encoded_data, overwrite=True, content_settings=my_content_settings, timeout=600)
            if verbose: logging.info(f"Checkpoint Saved to Azure: {len(data)} cards total.")
        except Exception as e:
            logging.error(f"WARNING: Failed to save checkpoint to Azure: {e}")

    # HELPER: Load Progress
    def load_existing_progress(filename):
        try:
            blob_client = get_blob_client(filename)
            if not blob_client.exists():
                logging.info("No existing file found. Starting fresh.")
                return []
                
            logging.info(f"Checking Azure for existing file: {filename}...")
            download_stream = blob_client.download_blob()
            data = json.loads(download_stream.readall())
            logging.info(f"Found existing file! Resuming with {len(data)} cards.")
            return data
        except Exception as e:
            logging.warning(f"Error reading existing blob (starting fresh): {e}")
            return []

    # MAIN EXTRACTION LOOP
    try:
        today_str = datetime.now().strftime("%Y%m%d")
        filename = f"pokemon_data_{today_str}.json"

        all_cards = load_existing_progress(filename)
        batch_size = 250
        start_page = (len(all_cards) // batch_size) + 1
        
        page = start_page
        
        while True:
            params = {'page': page, 'pageSize': batch_size}
            headers = {'X-Api-Key': API_KEY}

            try:
                logging.info(f"Requesting Page {page}...")
                response = http.get(BASE_URL, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                cards_on_page = data['data']
                
                if not cards_on_page:
                    logging.info("Done! (No more data from API)")
                    break
                    
                all_cards.extend(cards_on_page)

                if page % PAGES_PER_SAVE == 0:
                    upload_to_azure(all_cards, filename)

                if len(cards_on_page) < batch_size:
                    logging.info("End of list reached.")
                    break
                
                page += 1
                time.sleep(0.5)

            except Exception as e:
                logging.error(f"CRITICAL FAIL on Page {page}: {e}")
                logging.info("Attempting emergency save before exiting...")
                upload_to_azure(all_cards, filename)
                return False 

        logging.info("Extraction Finished. Performing final upload...")
        upload_to_azure(all_cards, filename)
        return True 

    except Exception as e:
        logging.error(f"Unhandled error in extraction: {e}")
        return False

def run_sql_load():
    # Using 'SqlConnectionString' from Environment Variables
    sql_conn_str = os.getenv("SqlConnectionString")
    
    if not sql_conn_str:
        logging.error("ERROR: 'SqlConnectionString' not found in Environment Variables.")
        return

    try:
        with pyodbc.connect(sql_conn_str) as conn:
            # IMPORTANT: Many stored procedures require autocommit if they 
            # manage their own internal transactions.
            conn.autocommit = True

            with conn.cursor() as cursor:
                logging.info("API Upload finished. Starting SQL Load...")
                
                # Executing procedures
                procedures = ["bronze.load_bronze", "silver.load_silver", "gold.load_gold"]
                
                for proc in procedures:
                    logging.info(f"Executing {proc}...")
                    cursor.execute(f"EXEC {proc}")
                    # In some environments, cursor.execute alone doesn't 'flush' 
                    # the command. This ensures it moves to the next.
                    while cursor.nextset(): 
                        pass
                
                logging.info("SQL Load Successful and Committed.")
                
    except Exception as e:
        logging.error(f"SQL Load failed: {e}")