import logging
import os
import time
import random
import datetime

import azure.functions as func
from azure.cosmos import CosmosClient
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

PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "60"))
MAX_ITEMS_PER_RUN = int(os.environ.get("MAX_ITEMS_PER_RUN", "2000"))
SEARCH_DAYS_BACK = int(os.environ.get("SEARCH_DAYS_BACK", "30"))

app = func.FunctionApp()

# ---------------------------------------------------------------------------
# HTTP SESSION SETUP
# ---------------------------------------------------------------------------
def get_session():
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "AzureFunction/1.2 (contact: admin@example.com)",
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
def utcnow_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()

def resolve_scrape_window():
    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=SEARCH_DAYS_BACK)
    end_date = today
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def fetch_with_backoff(url: str, params=None):
    for attempt in range(6):
        try:
            response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if response.status_code in (403, 429, 503):
                retry_after = response.headers.get("Retry-After")
                sleep_time = (
                    int(retry_after)
                    if (retry_after and retry_after.isdigit())
                    else (2 + attempt + random.random())
                )
                logging.warning("Rate limited for %s. Sleeping %.2fs", url, sleep_time)
                time.sleep(sleep_time)
                continue

            response.raise_for_status()
            return response

        except requests.HTTPError as exc:
            if attempt == 5:
                raise exc
            time.sleep(1.5 + attempt * 0.7 + random.random())
        except requests.RequestException as exc:
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
            dt = datetime.datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                return dt.isoformat()
            return dt.astimezone(datetime.timezone.utc).isoformat()
        except ValueError:
            continue

    try:
        dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.isoformat()
        return dt.astimezone(datetime.timezone.utc).isoformat()
    except Exception:
        return None

def get_judgment_date_for_ecli(ecli: str):
    try:
        response = fetch_with_backoff(BASE_CONTENT, {"id": ecli, "return": "META"})
        if not response:
            return None

        soup = BeautifulSoup(response.content, "xml")

        preferred_names = {
            "date",
            "issued",
            "issuedate",
            "judgmentdate",
            "publicationdate",
            "datum",
        }

        for tag in soup.find_all(True):
            if tag.name and tag.name.lower() in preferred_names:
                parsed = parse_date(tag.get_text(strip=True))
                if parsed:
                    return parsed

        date_node = soup.find(lambda tag: tag.name and "date" in tag.name.lower())
        if date_node:
            return parse_date(date_node.get_text(strip=True))

        return None

    except Exception as exc:
        logging.warning("Could not parse date for %s: %s", ecli, exc)
        return None

def fetch_full_text(ecli: str) -> str:
    try:
        response = fetch_with_backoff(BASE_CONTENT, {"id": ecli})
        return response.text if response else ""
    except Exception as exc:
        logging.error("Failed to fetch text for %s: %s", ecli, exc)
        return ""

def extract_clean_text(xml_content: str) -> str:
    if not xml_content:
        return ""

    try:
        soup = BeautifulSoup(xml_content, "xml")

        content_node = soup.find(
            lambda tag: tag.name and tag.name.lower() in ["uitspraak", "conclusie"]
        )

        if content_node:
            text = content_node.get_text(separator="\n\n", strip=True)
        else:
            paras = soup.find_all(
                lambda tag: tag.name and tag.name.lower() in ["para", "parablock", "title"]
            )
            if paras:
                text = "\n\n".join(
                    p.get_text(separator=" ", strip=True)
                    for p in paras
                    if p.get_text(strip=True)
                )
            else:
                text = soup.get_text(separator="\n\n", strip=True)

        return text[:500000]

    except Exception as exc:
        logging.warning("Failed to extract clean text: %s", exc)
        return ""

def item_exists(container, ecli: str) -> bool:
    query = "SELECT TOP 1 c.id FROM c WHERE c.id = @id"
    items = list(
        container.query_items(
            query=query,
            parameters=[{"name": "@id", "value": ecli}],
            enable_cross_partition_query=True,
        )
    )
    return len(items) > 0

# ---------------------------------------------------------------------------
# MAIN TIMER TRIGGER
# ---------------------------------------------------------------------------
@app.schedule(
    schedule="0 0 4 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def rechtspraak_scraper(myTimer: func.TimerRequest) -> None:
    logging.info("Rechtspraak Scraper started.")

    start_date_str, end_date_str = resolve_scrape_window()
    logging.info("Scraping window: %s to %s", start_date_str, end_date_str)

    try:
        if not COSMOS_ENDPOINT or not COSMOS_KEY:
            raise ValueError("COSMOS_ENDPOINT or COSMOS_KEY missing")
        if not BLOB_CONNECTION_STRING:
            raise ValueError("BLOB_CONNECTION_STRING missing")

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
    total_skipped = 0
    total_errors = 0

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
            if not response:
                logging.warning("No response returned for offset %s", offset)
                break

            soup = BeautifulSoup(response.content, "xml")
            entries = soup.find_all(lambda tag: tag.name and tag.name.lower() == "entry")

        except Exception as exc:
            logging.error("Critical error fetching feed: %s", exc)
            break

        if not entries:
            logging.info("No more entries found for this time window.")
            break

        logging.info("Processing %s items", len(entries))

        for entry in entries:
            try:
                id_node = entry.find(lambda tag: tag.name and tag.name.lower() == "id")
                title_node = entry.find(lambda tag: tag.name and tag.name.lower() == "title")
                summary_node = entry.find(lambda tag: tag.name and tag.name.lower() == "summary")

                ecli = id_node.get_text(strip=True) if id_node else ""
                title = title_node.get_text(strip=True) if title_node else ""
                summary = summary_node.get_text(strip=True) if summary_node else ""

                if not ecli:
                    continue

                if item_exists(container, ecli):
                    total_skipped += 1
                    logging.info("Skipping existing ECLI: %s", ecli)
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
                    "scraped_at": utcnow_iso(),
                }

                container.upsert_item(item)
                total_processed += 1
                logging.info("Upserted ECLI: %s", ecli)

                time.sleep(0.1)

                if total_processed >= MAX_ITEMS_PER_RUN:
                    logging.warning(
                        "Reached max processing limit for one run (%s).",
                        MAX_ITEMS_PER_RUN,
                    )
                    break

            except Exception as exc:
                total_errors += 1
                logging.error("Failed processing entry: %s", exc)

        if total_processed >= MAX_ITEMS_PER_RUN:
            break

        offset += PAGE_SIZE

    logging.info(
        "Run finished. Total upserted: %s, skipped existing: %s, errors: %s",
        total_processed,
        total_skipped,
        total_errors,
    )
