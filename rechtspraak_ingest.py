
#!/usr/bin/env python3

import os
import sys
import time
import datetime as dt
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from datasets import Dataset
from huggingface_hub import login

from name_scrubber import UltraNameScrubber

# Configuration
HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
API_URL_LIST = "https://data.rechtspraak.nl/uitspraken/zoeken"
API_URL_TEXT = "https://data.rechtspraak.nl/uitspraken/content"
LIMIT = int(os.environ.get("LIMIT", "300"))
SLEEP_BETWEEN = 1.0

# Load scrubber
JUDGE_FILE = Path(__file__).with_name("judge_names.json")
SCRUBBER = UltraNameScrubber(JUDGE_FILE)

def list_eclis(limit: int):
    today = dt.date.today()
    start = today - dt.timedelta(days=90)

    params = {
        "facet": "publicatiedatum",
        "publicatiedatum": f"{start.isoformat()}..{today.isoformat()}",
        "zaaknummer": "false",
        "max": str(limit),
        "output": "json"
    }

    response = requests.get(API_URL_LIST, params=params)
    print(f"🔗 Requested: {response.url}")
    response.raise_for_status()

    try:
        data = response.json()
        return [doc["id"].split("/")[-1] for doc in data.get("results", [])]
    except Exception as e:
        print(f"Failed to parse ECLI list: {e}", file=sys.stderr)
        return []

def fetch_text(ecli: str) -> str:
    response = requests.get(f"{API_URL_TEXT}/{ecli}")
    response.raise_for_status()
    root = ET.fromstring(response.text)
    return ET.tostring(root, encoding="unicode", method="text")

def main():
    login(os.environ["HF_TOKEN"])
    eclis = list_eclis(LIMIT)
    print(f"Fetched {len(eclis)} ECLI identifiers")

    records = []
    for ecli in eclis:
        try:
            raw_text = fetch_text(ecli)
            clean_text = SCRUBBER.scrub_names(raw_text)
            records.append({
                "url": f"https://data.rechtspraak.nl/uitspraken/content/{ecli}",
                "content": clean_text,
                "source": "rechtspraak"
            })
            time.sleep(SLEEP_BETWEEN)
        except Exception as e:
            print(f"Failed to process {ecli}: {e}", file=sys.stderr)

    if records:
        dataset = Dataset.from_list(records)
        dataset.push_to_hub(HF_REPO)
        print(f"Pushed {len(records)} court cases to {HF_REPO}")
    else:
        print("No records to push.")

if __name__ == "__main__":
    main()
