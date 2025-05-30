#!/usr/bin/env python3
"""
rechtspraak_ingest.py
─────────────────────
One-slice importer for Dutch court cases → Hugging Face Hub.

• Keeps progress in a local SQLite file (state.db) committed back to GitHub,
  so each GitHub Actions run resumes where the previous left off.
• Runs in two phases:
    – Back-fill: 30-day slices until yesterday is reached.
    – Daily:     1-day slice (yesterday) on every overnight run.
• Uses UltraNameScrubber from rechtspraak_to_hf.py to anonymise content.
"""

import os
import sys
import time
import json
import sqlite3
import datetime as dt
from pathlib import Path
from typing import List, Dict

import requests
import xml.etree.ElementTree as ET
from datasets import Dataset
from huggingface_hub import login

# ─────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────
HF_REPO        = "vGassen/dutch-court-cases-rechtspraak"

API_URL_LIST   = "https://data.rechtspraak.nl/uitspraken/zoeken"
API_URL_TEXT   = "https://data.rechtspraak.nl/uitspraken/content"

BACKLOG_SLICE  = 30          # days fetched per run while catching up
DAILY_SLICE    = 1           # days fetched once up-to-date
SLEEP_BETWEEN  = 1.0         # seconds between API calls to be polite
START_DATE     = dt.date(1999, 1, 1)

DB_PATH        = Path("state.db")
JUDGE_FILE     = Path(__file__).with_name("judge_names.json")

# ─────────────────────────────────────────────────────────────
#  SCRUBBER (imported from your existing file)
# ─────────────────────────────────────────────────────────────
from rechtspraak_to_hf import UltraNameScrubber

SCRUBBER = UltraNameScrubber(JUDGE_FILE)

# ─────────────────────────────────────────────────────────────
#  DATABASE UTILITIES
# ─────────────────────────────────────────────────────────────
def open_db() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH)
    db.execute("CREATE TABLE IF NOT EXISTS progress (last DATE)")
    db.execute("INSERT OR IGNORE INTO progress VALUES (?)", (START_DATE.isoformat(),))
    db.execute("CREATE TABLE IF NOT EXISTS seen_ecli (ecli TEXT PRIMARY KEY)")
    return db

def get_last_date(db: sqlite3.Connection) -> dt.date:
    (d,) = db.execute("SELECT last FROM progress").fetchone()
    return dt.date.fromisoformat(d)

def set_last_date(db: sqlite3.Connection, date_: dt.date) -> None:
    db.execute("UPDATE progress SET last = ?", (date_.isoformat(),))
    db.commit()

def already_seen(db: sqlite3.Connection, ecli: str) -> bool:
    return db.execute("SELECT 1 FROM seen_ecli WHERE ecli = ?", (ecli,)).fetchone() is not None

def mark_seen(db: sqlite3.Connection, ecli: str) -> None:
    db.execute("INSERT OR IGNORE INTO seen_ecli VALUES (?)", (ecli,))

# ─────────────────────────────────────────────────────────────
#  RECHTSPRAAK API HELPERS
# ─────────────────────────────────────────────────────────────
def list_eclis(date_from: str, date_to: str) -> List[str]:
    params = {
        "type": "uitspraak",
        "return": "DOC",
        "published_after":  date_from,
        "published_before": date_to,
        "max": 1000,                      # API hard-limit
    }
    r = requests.get(API_URL_LIST, params=params, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    return [
        entry.find("{http://www.w3.org/2005/Atom}id").text
        for entry in root.findall("{http://www.w3.org/2005/Atom}entry")
    ]

def fetch_text(ecli: str) -> str:
    r = requests.get(f"{API_URL_TEXT}?id={ecli}", timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    node = root.find(".//{http://www.rechtspraak.nl/schema/rechtspraak-1.0}uitspraak")
    return (
        ET.tostring(node, encoding="unicode", method="text").strip()
        if node is not None else ""
    )

# ─────────────────────────────────────────────────────────────
#  MAIN INGEST LOGIC (ONE SLICE)
# ─────────────────────────────────────────────────────────────
def ingest_slice(start: dt.date, span: int, db: sqlite3.Connection) -> None:
    end = start + dt.timedelta(days=span)
    print(f"[INFO] slice {start} → {end - dt.timedelta(days=1)}")

    batch: List[Dict] = []
    for ecli in list_eclis(start.isoformat(), end.isoformat()):
        if already_seen(db, ecli):
            continue

        time.sleep(SLEEP_BETWEEN)
        raw = fetch_text(ecli)
        if not raw:
            continue

        cleaned = SCRUBBER.scrub_names(raw)
        batch.append({"ecli": ecli, "uitspraak": cleaned})
        mark_seen(db, ecli)

        if len(batch) % 100 == 0:
            print(f"  processed {len(batch)} new rulings…")

    db.commit()
    set_last_date(db, end)

    if batch:
        tmp = Path("batch.jsonl")
        with tmp.open("w", encoding="utf-8") as f:
            for row in batch:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"[INFO] uploading {len(batch)} to Hugging Face")
        Dataset.from_json(str(tmp)).push_to_hub(
            HF_REPO, append=True, token=os.environ["HF_TOKEN"]
        )
        tmp.unlink()
    else:
        print("[INFO] no new rulings in this slice")

# ─────────────────────────────────────────────────────────────
#  ENTRY POINT  – run ONE slice per GitHub Actions job
# ─────────────────────────────────────────────────────────────
def main() -> None:
    token = os.getenv("HF_TOKEN")
    if not token:
        sys.exit("HF_TOKEN environment variable not set")
    login(token=token)

    db = open_db()
    last = get_last_date(db)
    today = dt.date.today()

    # decide slice size
    span = BACKLOG_SLICE if last < today - dt.timedelta(days=1) else DAILY_SLICE
    ingest_slice(last, span, db)
    db.close()

if __name__ == "__main__":
    main()
