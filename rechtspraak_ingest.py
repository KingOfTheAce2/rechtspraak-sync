#!/usr/bin/env python3
"""rechtspraak_ingest.py
───────────────────────
Fetches Dutch court rulings from Rechtspraak.nl, scrubs names,
and pushes results to Hugging Face. Tracks progress using `state.db`.
"""
from __future__ import annotations

import os
import sys
import time
import json
import sqlite3
import datetime as dt
from pathlib import Path
from typing import List
import xml.etree.ElementTree as ET

import requests
from datasets import Dataset
from huggingface_hub import login

from name_scrubber import UltraNameScrubber

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
HF_REPO       = "vGassen/dutch-court-cases-rechtspraak"
API_URL_LIST  = "https://data.rechtspraak.nl/uitspraken/zoeken"
API_URL_TEXT  = "https://data.rechtspraak.nl/uitspraken/content"
BACKLOG_SLICE = 30
DAILY_SLICE   = 1
SLEEP_BETWEEN = 1.0
START_DATE    = dt.date(1999, 1, 1)
LIMIT         = int(os.environ.get("LIMIT", "0"))

DB_PATH       = Path("state.db")
JUDGE_FILE    = Path(__file__).with_name("judge_names.json")
SCRUBBER      = UltraNameScrubber(JUDGE_FILE)

# ─────────────────────────────────────────────────────────────
# DB FUNCTIONS
# ─────────────────────────────────────────────────────────────
def _open_db() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH)
    db.execute("CREATE TABLE IF NOT EXISTS progress (last DATE)")
    db.execute("INSERT OR IGNORE INTO progress VALUES (?)", (START_DATE.isoformat(),))
    db.execute("CREATE TABLE IF NOT EXISTS seen_ecli (ecli TEXT PRIMARY KEY)")
    return db

def _get_last_date(db: sqlite3.Connection) -> dt.date:
    (d,) = db.execute("SELECT last FROM progress").fetchone()
    return dt.date.fromisoformat(d)

def _set_last_date(db: sqlite3.Connection, date_: dt.date) -> None:
    db.execute("UPDATE progress SET last = ?", (date_.isoformat(),))
    db.commit()

def _already_seen(db: sqlite3.Connection, ecli: str) -> bool:
    return False  # Overwrite always

def _mark_seen(db: sqlite3.Connection, ecli: str) -> None:
    db.execute("INSERT OR IGNORE INTO seen_ecli VALUES (?)", (ecli,))

# ─────────────────────────────────────────────────────────────
# RECHTSPRAAK API HELPERS
# ─────────────────────────────────────────────────────────────
def _list_eclis(date_from: str, date_to: str) -> List[str]:
    params = {
        "facet": "publicatiedatum",
        "zaaknummer": "false",
        "publicatiedatum": f"{date_from}..{date_to}",
        "max": "2000",
        "output": "json",
    }
    r = requests.get(API_URL_LIST, params=params)
    r.raise_for_status()
    data = r.json()
    return [doc["id"].split("/")[-1] for doc in data.get("results", [])]

def _fetch_ecli(ecli: str) -> str:
    r = requests.get(f"{API_URL_TEXT}/{ecli}")
    r.raise_for_status()
    root = ET.fromstring(r.text)
    return ET.tostring(root, encoding="unicode", method="text")

# ─────────────────────────────────────────────────────────────
# INGEST LOOP
# ─────────────────────────────────────────────────────────────
def main() -> None:
    login(os.environ["HF_TOKEN"])
    db = _open_db()
    today = dt.date.today()

    while True:
        date_from = _get_last_date(db)
        if date_from >= today:
            print("Ingestion complete.")
            break

        delta = BACKLOG_SLICE if (today - date_from).days > BACKLOG_SLICE else DAILY_SLICE
        date_to = date_from + dt.timedelta(days=delta)

        print(f"📅 Fetching cases: {date_from} → {date_to}")
        eclis = _list_eclis(date_from.isoformat(), date_to.isoformat())
        if LIMIT and len(eclis) > LIMIT:
            eclis = eclis[:LIMIT]
        print(f"📄 Found {len(eclis)} cases.")

        rows = []
        for ecli in eclis:
            try:
                raw = _fetch_ecli(ecli)
                clean = SCRUBBER.scrub_names(raw)
                rows.append({"ecli": ecli, "text": clean})
                _mark_seen(db, ecli)
                time.sleep(SLEEP_BETWEEN)
            except Exception as ex:
                print(f"⚠️  Skipped {ecli}: {ex}", file=sys.stderr)

        if rows:
            print(f"⬆️  Pushing {len(rows)} to Hugging Face: {HF_REPO}")
            dataset = Dataset.from_list(rows)
            dataset.push_to_hub(HF_REPO)

        _set_last_date(db, date_to)

if __name__ == "__main__":
    main()
