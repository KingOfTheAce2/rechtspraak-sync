#!/usr/bin/env python3
# rechtspraak_ingest.py  – run once, it handles backlog + daily delta

import os, sys, time, json, sqlite3, argparse, datetime as dt
import requests, xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict
from datasets import Dataset
from huggingface_hub import login

# ───────────────────  CONFIG  ───────────────────
HF_REPO       = "vGassen/dutch-court-cases-rechtspraak"
API_URL_LIST  = "https://data.rechtspraak.nl/uitspraken/zoeken"
API_URL_TEXT  = "https://data.rechtspraak.nl/uitspraken/content"

SLICE_DAYS    = 30          # backlog window
SLEEP_CALL    = 1.0         # seconds between API calls
RUN_HOUR      = 2           # time of day (24 h) for the daily check
START_DATE    = dt.date(1999, 1, 1)

# ───────────────────  SCRUBBER  ─────────────────
from ultra_scrubber import UltraNameScrubber     # put the class in ultra_scrubber.py

judge_file = Path(__file__).with_name("judge_names.json")
SCRUBBER   = UltraNameScrubber(judge_file)

# ───────────────────  STATE DB  ─────────────────
DB = Path(__file__).with_name("state.db")

def db_conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB)
    c.execute("CREATE TABLE IF NOT EXISTS progress (last DATE)")
    c.execute("INSERT OR IGNORE INTO progress VALUES (?)", (START_DATE.isoformat(),))
    c.execute("CREATE TABLE IF NOT EXISTS seen_ecli (ecli TEXT PRIMARY KEY)")
    c.commit()
    return c

def get_last_date(c: sqlite3.Connection) -> dt.date:
    row = c.execute("SELECT last FROM progress").fetchone()
    return dt.date.fromisoformat(row[0])

def set_last_date(c: sqlite3.Connection, d: dt.date) -> None:
    c.execute("UPDATE progress SET last = ?", (d.isoformat(),))
    c.commit()

def seen(c: sqlite3.Connection, ecli: str) -> bool:
    return c.execute("SELECT 1 FROM seen_ecli WHERE ecli = ?", (ecli,)).fetchone() is not None

def mark_seen(c: sqlite3.Connection, ecli: str) -> None:
    c.execute("INSERT OR IGNORE INTO seen_ecli VALUES (?)", (ecli,))

# ───────────────────  API  ──────────────────────
def list_eclis(d_from: str, d_to: str) -> List[str]:
    params = {
        "type": "uitspraak", "return": "DOC",
        "published_after": d_from, "published_before": d_to,
        "max": 1000,
    }
    r = requests.get(API_URL_LIST, params=params, timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    return [e.find("{http://www.w3.org/2005/Atom}id").text
            for e in root.findall("{http://www.w3.org/2005/Atom}entry")]

def get_text(ecli: str) -> str:
    r = requests.get(f"{API_URL_TEXT}?id={ecli}", timeout=30)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    ns = {"rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0"}
    node = root.find(".//rs:uitspraak", ns)
    return ET.tostring(node, encoding="unicode", method="text").strip() if node is not None else ""

# ───────────────────  UPLOAD  ───────────────────
def push_batch(rows: List[Dict]) -> None:
    if not rows:
        return
    tmp = Path("batch.jsonl")
    with tmp.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    Dataset.from_json(str(tmp)).push_to_hub(HF_REPO, append=True)
    tmp.unlink()

# ───────────────────  ONE SLICE  ────────────────
def ingest_slice(start: dt.date, span: int, c: sqlite3.Connection) -> dt.date:
    end = start + dt.timedelta(days=span)
    print(f"[INFO] slice {start} → {end - dt.timedelta(days=1)}")

    rows = []
    for ecli in list_eclis(start.isoformat(), end.isoformat()):
        if seen(c, ecli):
            continue
        time.sleep(SLEEP_CALL)
        txt = get_text(ecli)
        if not txt:
            continue
        cleaned = SCRUBBER.scrub_names(txt)
        rows.append({"ecli": ecli, "uitspraak": cleaned})
        mark_seen(c, ecli)

        if len(rows) % 100 == 0:
            print(f"  processed {len(rows)} new")

    push_batch(rows)
    set_last_date(c, end)
    print(f"[INFO] uploaded {len(rows)} new rulings")
    return end

# ───────────────────  MAIN LOOP  ───────────────
def run_forever() -> None:
    token = os.getenv("HF_TOKEN")
    if not token:
        sys.exit("HF_TOKEN not set")
    login(token=token)

    conn = db_conn()

    while True:
        today = dt.date.today()
        last  = get_last_date(conn)

        # backlog mode
        if last < today - dt.timedelta(days=1):
            ingest_slice(last, SLICE_DAYS, conn)
            continue

        # caught up → wait until RUN_HOUR next day
        now = dt.datetime.now()
        next_run = dt.datetime.combine(today + dt.timedelta(days=1),
                                       dt.time(hour=RUN_HOUR))
        wait = (next_run - now).total_seconds()
        print(f"[INFO] up to date – sleeping {int(wait/3600)} h")
        time.sleep(wait)

# ───────────────────  ENTRY  ───────────────────
if __name__ == "__main__":
    run_forever()
