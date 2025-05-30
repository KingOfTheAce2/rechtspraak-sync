#!/usr/bin/env python3
"""
push_250_sample.py — minimal sample uploader
─────────────────────────────────────────────
Fetch **250** recent Dutch court rulings from the open Rechtspraak API, scrub
personal names with *UltraNameScrubber*, and publish the cleaned sample to a
Hugging Face dataset repo.

Usage (one‑liner):
    HF_TOKEN=… HF_REPO="username/test-rechtspraak" python push_250_sample.py

Dependencies:
    pip install requests datasets huggingface_hub

Environment variables:
    • HF_TOKEN  – required; write token for the Hub
    • HF_REPO   – target repo (defaults to "username/rs-sample")

The script does *no* state keeping: every run uploads a fresh 250‑item sample.
"""
from __future__ import annotations

import os
import sys
import requests
import datetime as dt
from pathlib import Path
from typing import List, Tuple

from datasets import Dataset
from huggingface_hub import login

from name_scrubber import UltraNameScrubber

# ─────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────
API_URL_LIST = "https://data.rechtspraak.nl/uitspraken/zoeken"
API_URL_TEXT = "https://data.rechtspraak.nl/uitspraken/content"
MAX_CASES    = 250
JUDGE_FILE   = Path(__file__).with_name("judge_names.json")
SCRUBBER     = UltraNameScrubber(JUDGE_FILE)

# ─────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────

def fetch_latest_eclis(limit: int) -> list[str]:
    today = dt.date.today()
    start = today - dt.timedelta(days=90)
    end = today - dt.timedelta(days=1)

    params = {
        "output": "json",
        "max": str(limit),
        "sort": "datum desc",
        "publicatiedatum": f"{start.isoformat()}..{end.isoformat()}",
        "facet": "publicatiedatum"
    }

    r = requests.get(API_URL_LIST, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return [item["id"].split("/")[-1] for item in data.get("results", [])]

def fetch_full_text(ecli: str) -> str:
    """Download raw XML string for one ruling."""
    r = requests.get(f"{API_URL_TEXT}/{ecli}", timeout=30)
    r.raise_for_status()
    return r.text

# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def main() -> None:
    hf_repo   = os.getenv("HF_REPO", "username/rs-sample")
    hf_token  = os.getenv("HF_TOKEN")
    if not hf_token:
        sys.exit("Set HF_TOKEN env var with your Hugging Face write token.")

    login(token=hf_token)

    print(f"Fetching metadata for {MAX_CASES} rulings …")
    cases = fetch_latest_eclis(MAX_CASES)

    records = []
    for idx, (ecli, date) in enumerate(cases, 1):
        try:
            raw_xml    = fetch_full_text(ecli)
            clean_text = SCRUBBER.scrub_names(raw_xml)
            records.append({"ecli": ecli, "date": date, "text": clean_text})
            if idx % 25 == 0:
                print(f"  ✔ {idx}/{MAX_CASES} done")
        except Exception as exc:
            print(f"⚠️ Skipping {ecli}: {exc}", file=sys.stderr)

    print(f"Creating Dataset with {len(records)} items …")
    ds = Dataset.from_list(records)

    print(f"Pushing to Hugging Face dataset repo '{hf_repo}' …")
    ds.push_to_hub(hf_repo, split="train")
    print("✅ Upload complete!")


if __name__ == "__main__":
    main()
