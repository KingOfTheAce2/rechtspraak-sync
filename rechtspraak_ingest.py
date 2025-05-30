#!/usr/bin/env python3
"""rechtspraak_ingest.py
───────────────────────
Incremental loader that pulls Dutch court rulings from the open
*Rechtspraak* API, scrubs personal names with **UltraNameScrubber**
(`name_scrubber.py`), and pushes the cleaned data set to the Hugging Face Hub.

The script is designed to run inside GitHub Actions on a daily schedule, yet
is also safe to execute locally for ad-hoc re-runs.  Progress is tracked in a
SQLite file (`state.db`) that is committed back to the repo so that every run
continues exactly where the previous one left off.

**Phases**
 1. *Back-fill*: fetches 30-day slices until it reaches yesterday.
 2. *Daily*:   fetches a one-day slice (yesterday) on every subsequent run.

Environment variable **`HF_TOKEN`** must contain a write token for the HF Hub.
"""
from __future__ import annotations

import os
import sys
import time
import json
import sqlite3
import datetime as dt
from pathlib import Path
from typing import List, Dict
import xml.etree.ElementTree as ET

import requests
from datasets import Dataset
from huggingface_hub import login

# ─────────────────────────────────────────────────────────────
#  LOCAL SCRUBBER
# ─────────────────────────────────────────────────────────────
from name_scrubber import UltraNameScrubber  # ← updated import

# ─────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────
HF_REPO       = "vGassen/dutch-court-cases-rechtspraak"
API_URL_LIST  = "https://data.rechtspraak.nl/uitspraken/zoeken"
API_URL_TEXT  = "https://data.rechtspraak.nl/uitspraken/content"

BACKLOG_SLICE = 30      # days per run while catching up
DAILY_SLICE   = 1       # days per run once current
SLEEP_BETWEEN = 1.0     # polite delay between content calls (seconds)
START_DATE    = dt.date(1999, 1, 1)

DB_PATH       = Path("state.db")
JUDGE_FILE    = Path(__file__).with_name("judge_names.json")
SCRUBBER      = UltraNameScrubber(JUDGE_FILE)

# ─────────────────────────────────────────────────────────────
#  DATABASE UTILITIES
# ─────────────────────────────────────────────────────────────

def _open_db() -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH)
    db.execute("CREATE TABLE IF NOT EXISTS progress (last DATE)")
    db.execute(
        "INSERT OR IGNORE INTO progress VALUES (?)",
        (START_DATE.isoformat(),),
    )
    db.execute("CREATE TABLE IF NOT EXISTS seen_ecli (ecli TEXT PRIMARY KEY)")
    return db


def _get_last_date(db: sqlite3.Connection) -> dt.date:
    (d,) = db.execute("SELECT last FROM progress").fetchone()
    return dt.date.fromisoformat(d)


def _set_last_date(db: sqlite3.Connection, date_: dt.date) -> None:
    db.execute("UPDATE progress SET last = ?", (date_.isoformat(),))
    db.commit()


def _already_seen(db: sqlite3.Connection, ecli: str) -> bool:
    return (
        db.execute("SELECT 1 FROM seen_ecli WHERE ecli = ?", (ecli,)).fetchone()
        is not None
    )


def _mark_seen(db: sqlite3.Connection, ecli: str) -> None:
    db.execute("INSERT OR IGNORE INTO seen_ecli VALUES (?)", (ecli,))

# ─────────────────────────────────────────────────────────────
#  RECHTSPRAAK API HELPERS
# ─────────────────────────────────────────────────────────────

def _list_eclis(date_from: str, date_to: str) -> List[str]:
    """Return all ECLIs published in [date_from, date_to)."""
    params = {
        "type": "uitspraak",
        "return": "DOC",
        "published_after": date_from,
        "published_before": date_to,
        "max": 1000,  # API hard limit per request
    }
    r = requests.get(API_URL_LIST, params=params, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    return [
        entry.find("{http://www.w3.org/2005/Atom}id").text
        for entry in root.findall("{http://www.w3.org/2005/Atom}entry")
    ]


def _fetch_text(ecli: str) -> str:
    """Download the plain-text version of a ruling by ECLI."""
    r = requests.get(f"{API_URL_TEXT}?id={ecli}", timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    node = root.find(
        ".//{http://www.rechtspraak.nl/schema/rechtspraak-1.0}uitspraak"
    )
    return (
        ET.tostring(node, encoding="unicode", method="text").strip()
        if node is not None
        else ""
    )

# ─────────────────────────────────────────────────────────────
#  INGEST – ONE SLICE
# ─────────────────────────────────────────────────────────────

def _ingest_slice(start: dt.date, span: int, db: sqlite3.Connection) -> None:
    """Fetch, scrub and upload one continuous slice (`span` days) of data."""
    end = start + dt.timedelta(days=span)
    print(f"[INFO] slice {start} → {end - dt.timedelta(days=1)}")

    batch: List[Dict] = []
    for ecli in _list_eclis(start.isoformat(), end.isoformat()):
        if _already_seen(db, ecli):
            continue

        time.sleep(SLEEP_BETWEEN)
        raw = _fetch_text(ecli)
        if not raw:
            continue

        cleaned = SCRUBBER.scrub_names(raw)
        batch.append({"ecli": ecli, "uitspraak": cleaned})
        _mark_seen(db, ecli)

        if len(batch) % 100 == 0:
            print(f"  processed {len(batch)} new rulings…")

    db.commit()
    _set_last_date(db, end)

    if not batch:
        print("[INFO] no new rulings in this slice")
        return

    tmp = Path("batch.jsonl")
    with tmp.open("w", encoding="utf-8") as f:
        for row in batch:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"[INFO] uploading {len(batch)} rulings → {HF_REPO}")
    Dataset.from_json(str(tmp)).push_to_hub(
        HF_REPO, append=True, token=os.environ["HF_TOKEN"]
    )
    tmp.unlink()

# ─────────────────────────────────────────────────────────────
#  ENTRY POINT (one slice per CI run)
# ─────────────────────────────────────────────────────────────

def main() -> None:
    token = os.getenv("HF_TOKEN")
    if not token:
        sys.exit("HF_TOKEN environment variable not set")
    login(token=token)

    db = _open_db()
    last = _get_last_date(db)
    today = dt.date.today()

    span = BACKLOG_SLICE if last < today - dt.timedelta(days=1) else DAILY_SLICE
    _ingest_slice(last, span, db)
    db.close()


if __name__ == "__main__":
    main()
