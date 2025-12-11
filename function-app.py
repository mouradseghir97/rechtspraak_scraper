import logging
import os
import time
import random
import datetime
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

import azure.functions as func
from azure.cosmos import CosmosClient, PartitionKey
from azure.storage.blob import BlobServiceClient
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# Env Vars (set these in Azure Portal)
COSMOS_ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
COSMOS_KEY = os.environ.get("COSMOS_KEY")
COSMOS_DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME", "RechtspraakDB")
COSMOS_CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER_NAME", "Uitspraken")

BLOB_CONNECTION_STRING = os.environ.get("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "rechtspraak-xml")

# Scraper Settings
# Defaulting to 2025 to keep execution time manageable. Change via App Settings.
START_DATE = os.environ.get("SEARCH_START_DATE", "2025-01-01")
END_DATE = os.environ.get("SEARCH_END_DATE", "2025-12-31")
SUBJECT = os.environ.get("SEARCH_SUBJECT", "http://psi.rechtspraak.nl/rechtsgebied#bestuursrecht_belastingrecht")

BASE_SEARCH = "https://data.rechtspraak.nl/uitspraken/zoeken"
BASE_CONTENT = "https://data.rechtspraak.nl/uitspraken/content"

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
DCT_NS = {"dcterms": "http://purl.org/dc/terms/"}

PAGE_SIZE = 100
REQUEST_TIMEOUT = 60

app = func.FunctionApp()

# ---------------------------------------------------------------------------
# HTTP SESSION SETUP
# ---------------------------------------------------------------------------
def get_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "AzureFunction/1.0 (contact: admin@example.com)",
        "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
    })
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[403, 429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

session = get_session()

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------
def fetch_with_backoff(url: str, params: dict | list | None = None):
    """Fetches URL with robust retry and backoff logic."""
    for attempt in range(6):
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            
            # Custom handling for specific rate limits
            if r.status_code in (403, 429, 503):
                ra = r.headers.get("Retry-After")
                sleep_time = int(ra) if (ra and ra.isdigit()) else (2 + attempt + random.random())
                logging.warning(f"⚠️ Rate limited. Sleeping {sleep_time}s")
                time.sleep(sleep_time)
                continue
            
            r.raise_for_status()
            return r
        except requests.HTTPError as e:
            if attempt == 5:
                raise e
            time.sleep(1.5 + attempt * 0.7 + random.random())
    return None

def parse_date(text: str):
    if not text:
        return None
    txt = text.strip()
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.datetime.strptime(txt, fmt).isoformat()
        except ValueError:
            pass
    return None

def get_judgment_date_for_ecli(ecli: str):
    """Fetches metadata to extract the exact judgment date."""
    try:
        r = fetch_with_backoff(BASE_CONTENT, {"id": ecli, "return": "META"})
        root = ET.fromstring(r.content)
        dt_text = root.findtext(".//dcterms:date", namespaces=DCT_NS)
        return parse_date(dt_text)
    except Exception as e:
        logging.warning(f"Could not parse date for {ecli}: {e}")
        return None

def fetch_full_text(ecli: str) -> str:
    """Fetches the full XML content of the judgment."""
    try:
        r = fetch_with_backoff(BASE_CONTENT, {"id": ecli})
        return r.text  # Storing the raw XML text
    except Exception as e:
        logging.error(f"Failed to fetch text for {ecli}: {e}")
        return ""

# ---------------------------------------------------------------------------
# MAIN TIMER TRIGGER
# ---------------------------------------------------------------------------
@app.schedule(schedule="0 0 4 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def rechtspraak_scraper(myTimer: func.TimerRequest) -> None:
    logging.info('🚀 Rechtspraak Scraper started.')

    # 1. Initialize Azure Clients
    try:
        # Cosmos DB
        cosmos_client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
        database = cosmos_client.get_database_client(COSMOS_DATABASE_NAME)
        container = database.get_container_client(COSMOS_CONTAINER_NAME)

        # Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        blob_container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
        if not blob_container_client.exists():
            blob_container_client.create_container()

    except Exception as e:
        logging.error(f"❌ Failed to initialize Azure Clients. Check env vars. Error: {e}")
        return

    # 2. Start Ingestion Loop
    offset = 0
    total_processed = 0
    
    while True:
        logging.info(f"🔎 Fetching feed offset: {offset}")
        
        # Build Search Query
        params = [
            ("subject", SUBJECT),
            ("max", PAGE_SIZE),
            ("from", offset),
            ("date", START_DATE),
            ("date", END_DATE),
            ("return", "DOC"),
            ("sort", "ASC"), # Oldest first
        ]
        
        try:
            r = fetch_with_backoff(BASE_SEARCH, params)
            root = ET.fromstring(r.content)
            entries = root.findall("atom:entry", ATOM_NS)
        except Exception as e:
            logging.error(f"❌ Critical error fetching feed: {e}")
            break

        if not entries:
            logging.info("✅ No more entries found. Scraping complete.")
            break

        logging.info(f"Processing {len(entries)} items...")

        for entry in entries:
            ecli = (entry.findtext("atom:id", namespaces=ATOM_NS) or "").strip()
            title = (entry.findtext("atom:title", namespaces=ATOM_NS) or "").strip()
            summary = (entry.findtext("atom:summary", namespaces=ATOM_NS) or "").strip()

            if not ecli:
                continue

            # --- Check if exists in Cosmos (Idempotency) ---
            # To save time/money, you can skip if already exists.
            # Comment this block out if you want to force-update every time.
            try:
                container.read_item(item=ecli, partition_key=ecli)
                logging.info(f"Skipping existing ECLI: {ecli}")
                continue 
            except Exception:
                pass # Item does not exist, proceed

            # --- 1. Fetch Metadata (Date) ---
            judgment_date = get_judgment_date_for_ecli(ecli)
            
            # --- 2. Fetch Full Text ---
            full_text_xml = fetch_full_text(ecli)
            
            blob_url = None
            if full_text_xml:
                # --- 3. Upload to Blob ---
                blob_filename = f"{ecli}.xml"
                try:
                    blob_client = blob_container_client.get_blob_client(blob_filename)
                    blob_client.upload_blob(full_text_xml, overwrite=True)
                    blob_url = blob_client.url
                except Exception as e:
                    logging.error(f"Blob upload failed for {ecli}: {e}")

            # --- 4. Upsert to Cosmos ---
            item = {
                "id": ecli, # Partition Key
                "ecli": ecli,
                "title": title,
                "summary": summary,
                "judgment_date": judgment_date,
                "blob_url": blob_url,
                "subject": SUBJECT,
                "scraped_at": datetime.datetime.utcnow().isoformat()
            }
            
            try:
                container.upsert_item(item)
                total_processed += 1
            except Exception as e:
                logging.error(f"Cosmos upsert failed for {ecli}: {e}")

            # Polite delay between items
            time.sleep(0.1)

        offset += PAGE_SIZE
        
        # Safety break for Function execution time limits (optional but recommended)
        # if total_processed > 2000: break 

    logging.info(f"🏁 Run Finished. Total documents processed: {total_processed}")