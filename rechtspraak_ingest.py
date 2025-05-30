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

LIMIT = int(os.environ.get("LIMIT", "0"))

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
