# src/config.py

import os
from pathlib import Path

# --- API Configuration ---
BASE_API_URL = "http://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_API_URL = "http://data.rechtspraak.nl/uitspraken/content"
DEEPLINK_URL_PREFIX = "http://deeplink.rechtspraak.nl/uitspraak?id="
API_MAX_RESULTS_PER_PAGE = 1000  # As per API documentation

# --- Local File Paths ---
CRAWLER_DIR = Path(__file__).parent.parent
DATA_DIR = CRAWLER_DIR / "data"
STATE_FILE = DATA_DIR / "crawler_state.json"
LOG_FILE = CRAWLER_DIR / "crawler.log"

# --- Hugging Face Hub Configuration ---
# Example: "YourUsername/YourDatasetName"
HF_DATASET_ID = os.getenv("HF_DATASET_ID", "vGassen/dutch-court-cases-rechtspraak")
HF_DATASET_PRIVATE = os.getenv("HF_DATASET_PRIVATE", "False").lower() in ("true", "1")


# --- Backfill Configuration ---
BACKFILL_MAX_ITEMS = int(os.getenv("BACKFILL_MAX_ITEMS", "10000"))


# --- Create necessary directories ---
DATA_DIR.mkdir(exist_ok=True)
