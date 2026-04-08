import logging
import os
import time
import random
import datetime
import xml.etree.ElementTree as ET

import azure.functions as func
from azure.cosmos import CosmosClient
from azure.cosmos import exceptions as cosmos_exceptions
from azure.storage.blob import BlobServiceClient
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
COSMOS_ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
COSMOS_KEY = os.environ.get("COSMOS_KEY")
COSMOS_DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME", "RechtspraakDB")
COSMOS_CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER_NAME", "Uitspraken")

BLOB_CONNECTION_STRING = os.environ.get("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "rechtspraak-xml")

SUBJECT = os.environ.get(
    "SEARCH_SUBJECT",
    "http://psi.rechtspraak.nl/rechtsgebied#bestuursrecht_belastingrecht",
)

BASE_SEARCH = "https://data.rechtspraak.nl/uitspraken/zoeken"
BASE_CONTENT = "https://data.rechtspraak.nl/uitspraken/content"

ATOM_NS = {"atom": "http://www.w3.org/2005/Atom"}
DCT_NS = {"dcterms": "http://purl.org/dc/terms/"}

PAGE_SIZE = 100
REQUEST_TIMEOUT = 60
MAX_ITEMS_PER_RUN = int(os.environ.get("MAX_ITEMS_PER_RUN", "2000"))

app = func.FunctionApp()

# ---------------------------------------------------------------------------
# HTTP SESSION SETUP
# ---------------------------------------------------------------------------
def get_session():
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "AzureFunction/1.1 (contact: admin@example.com)",
            "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
        }
    )
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
# HELPERS
# ---------------------------------------------------------------------------
def fetch_with_backoff(url: str, params=None):
    for attempt in range(6):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if response.status_code in (403, 429, 503):
                retry_after = response.headers.get("Retry-After")
                sleep_time = int(retry_after) if (retry_after and retry_after.isdigit()) else (2 + attempt + random.random())
                logging.warning("Rate limited for %s. Sleeping %.2fs", url, sleep_time)
                time.sleep(sleep_time)
                continue

            response.raise_for_status()
            return response
        except requests.HTTPError as exc:
            if attempt == 5:
                raise exc
            time.sleep(1.5 + attempt * 0.7 + random.random())
    return None


def parse_date(text: str):
    if not text:
        return None

    raw = text.strip()
    formats = (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    )
    for fmt in formats:
        try:
            return datetime.datetime.strptime(raw, fmt).isoformat()
        except ValueError:
            continue

    try:
        return datetime.datetime.fromisoformat(raw.replace("Z", "+00:00")).isoformat()
    except Exception:
        return None


def get_judgment_date_for_ecli(ecli: str):
    try:
        response = fetch_with_backoff(BASE_CONTENT, {"id": ecli, "return": "META"})
        root = ET.fromstring(response.content)
        dt_text = root.findtext(".//dcterms:date", namespaces=DCT_NS)
        return parse_date(dt_text)
    except Exception as exc:
        logging.warning("Could not parse date for %s: %s", ecli, exc)
        return None


def fetch_full_text(ecli: str) -> str:
    try:
        response = fetch_with_backoff(BASE_CONTENT, {"id": ecli})
        return response.text
    except Exception as exc:
        logging.error("Failed to fetch text for %s: %s", ecli, exc)
        return ""


def extract_clean_text(xml_content: str) -> str:
    if not xml_content:
        return ""

    try:
        soup = BeautifulSoup(xml_content, "xml")
        content_node = soup.find("uitspraak") or soup.find("conclusie")

        if content_node:
            text = content_node.get_text(separator="\n\n", strip=True)
        else:
            text = soup.get_text(separator="\n\n", strip=True)

        return text[:500000]
    except Exception as exc:
        logging.warning("Failed to extract clean text: %s", exc)
        return ""


def safe_partition_key(ecli: str) -> str:
    # Adjust this only if your Cosmos container partition key is not /id.
    return ecli


# ---------------------------------------------------------------------------
# MAIN TIMER TRIGGER
# ---------------------------------------------------------------------------
@app.schedule(schedule="0 0 4 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def rechtspraak_scraper(myTimer: func.TimerRequest) -> None:
    logging.info("Rechtspraak Scraper started.")

    today = datetime.date.today()
    default_start = (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    default_end = today.strftime("%Y-%m-%d")

    start_date_str = os.environ.get("SEARCH_START_DATE", default_start)
    end_date_str = os.environ.get("SEARCH_END_DATE", default_end)

    logging.info("Scraping window: %s to %s", start_date_str, end_date_str)

    try:
        cosmos_client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
        database = cosmos_client.get_database_client(COSMOS_DATABASE_NAME)
        container = database.get_container_client(COSMOS_CONTAINER_NAME)

        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        blob_container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
        if not blob_container_client.exists():
            blob_container_client.create_container()
    except Exception as exc:
        logging.error("Failed to initialize Azure clients: %s", exc)
        return

    offset = 0
    total_processed = 0

    while True:
        logging.info("Fetching feed offset: %s", offset)

        params = [
            ("subject", SUBJECT),
            ("max", PAGE_SIZE),
            ("from", offset),
            ("date", start_date_str),
            ("date", end_date_str),
            ("return", "DOC"),
            ("sort", "DESC"),
        ]

        try:
            response = fetch_with_backoff(BASE_SEARCH, params)
            root = ET.fromstring(response.content)
            entries = root.findall("atom:entry", ATOM_NS)
        except Exception as exc:
            logging.error("Critical error fetching feed: %s", exc)
            break

        if not entries:
            logging.info("No more entries found for this time window.")
            break

        logging.info("Processing %s items", len(entries))

        for entry in entries:
            ecli = (entry.findtext("atom:id", namespaces=ATOM_NS) or "").strip()
            title = (entry.findtext("atom:title", namespaces=ATOM_NS) or "").strip()
            summary = (entry.findtext("atom:summary", namespaces=ATOM_NS) or "").strip()

            if not ecli:
                continue

            try:
                container.read_item(item=ecli, partition_key=safe_partition_key(ecli))
                logging.info("Skipping existing ECLI: %s", ecli)
                continue
            except cosmos_exceptions.CosmosResourceNotFoundError:
                pass
            except Exception as exc:
                logging.error("Unexpected Cosmos read error for %s: %s", ecli, exc)
                continue

            judgment_date = get_judgment_date_for_ecli(ecli)
            full_text_xml = fetch_full_text(ecli)
            clean_text = extract_clean_text(full_text_xml)

            blob_url = None
            if full_text_xml:
                blob_filename = f"{ecli}.xml"
                try:
                    blob_client = blob_container_client.get_blob_client(blob_filename)
                    blob_client.upload_blob(full_text_xml, overwrite=True)
                    blob_url = blob_client.url
                except Exception as exc:
                    logging.error("Blob upload failed for %s: %s", ecli, exc)

            public_url = f"https://uitspraken.rechtspraak.nl/details?id={ecli}"

            item = {
                "id": ecli,
                "ecli": ecli,
                "title": title,
                "summary": summary,
                "full_text": clean_text,
                "text_length": len(clean_text),
                "judgment_date": judgment_date,
                "published_at": judgment_date,
                "date": judgment_date,
                "public_url": public_url,
                "source_url": public_url,
                "blob_xml_url": blob_url,
                "subject": SUBJECT,
                "source_domain": "rechtspraak.nl",
                "scraped_at": datetime.datetime.utcnow().isoformat(),
            }

            try:
                container.upsert_item(item)
                total_processed += 1
            except Exception as exc:
                logging.error("Cosmos upsert failed for %s: %s", ecli, exc)

            time.sleep(0.1)

            if total_processed >= MAX_ITEMS_PER_RUN:
                logging.warning("Reached max processing limit for one run (%s).", MAX_ITEMS_PER_RUN)
                break

        if total_processed >= MAX_ITEMS_PER_RUN:
            break

        offset += PAGE_SIZE

    logging.info("Run finished. Total documents processed: %s", total_processed)
