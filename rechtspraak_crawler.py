"""Utility script to fetch and upload Rechtspraak court decisions."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, TYPE_CHECKING, Any

try:
    import requests  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - handled in _session
    requests = None  # type: ignore

if TYPE_CHECKING:  # pragma: no cover
    import requests as requests_module

try:
    from datasets import Dataset  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - handled in main
    Dataset = None  # type: ignore

try:
    from huggingface_hub import login  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - handled in main
    login = None  # type: ignore

CHECKPOINT_FILE = "checkpoint.json"
HF_REPO = "vGassen/dutch-public-domain-texts"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"
USER_AGENT = "RechtspraakCrawler/1.0 (example@example.org)"

DEFAULT_DELAY = 1.0
REQUEST_PAUSE = DEFAULT_DELAY

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Load judge name list relative to this file
NAMES_PATH = os.path.join(os.path.dirname(__file__), "judge_names.json")
with open(NAMES_PATH, "r", encoding="utf-8") as f:
    JUDGE_NAMES = json.load(f)


def load_checkpoint() -> Dict[str, object]:
    """Load crawler state from disk."""

    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_published": None, "current_offset": 0}


def save_checkpoint(state: Dict[str, object]) -> None:
    """Persist crawler state to disk."""

    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _session() -> "requests.Session":
    """Configure and return a reusable :class:`requests.Session`."""

    if requests is None:
        raise ImportError(
            "The 'requests' package is required to run the crawler. "
            "Install dependencies via 'pip install -r requirements.txt'."
        )

    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/xml"})
    s.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))
    return s


NAME_REGEX = re.compile(
    r"(\(?)(?:[A-Z]\.? ?){1,4}(?:van den |van der |van |de |den )?[A-Z][a-z]+(?:-[A-Z][a-z]+)?(\)?)"
)


def scrub_names(text: str) -> str:
    """Remove personal names and signatures from a decision."""

    signature_markers = ["(get.)", "w.g.", "(getekend)"]

    lines = text.splitlines()
    clean_lines = []

    for line in lines:
        for full_name in JUDGE_NAMES:
            parts = full_name.split()
            if len(parts) < 2:
                continue
            prefix = " ".join(
                p for p in parts if "." not in p and p.lower() in {"mr.", "dhr.", "mw.", "prof.", "mr.drs.", "mr.dr.", "dr.", "drs."}
            )
            name_part = full_name.replace(prefix, "").strip()
            if name_part and name_part in line:
                line = line.replace(name_part, "[NAAM]")

        line = NAME_REGEX.sub(r"\1[NAAM]\2", line)
        for marker in signature_markers:
            line = line.replace(marker, "")

        if re.search(r"aldus vastgesteld|in tegenwoordigheid van|deze uitspraak|getekend", line, re.IGNORECASE):
            continue

        line = re.sub(r"\s+", " ", line).strip()
        line = re.sub(r"^[,.:;-]+", "", line).strip()

        if line:
            clean_lines.append(line)

    return "\n".join(clean_lines).strip()


def fetch_ecli_page(session: "requests.Session", since: str | None, offset: int) -> List[Dict[str, str]]:
    """Fetch a single page of ECLIs starting at ``offset``."""

    params = {
        "type": "uitspraak",
        "return": "DOC",
        "max": 1000,
        "from": offset,
        "sort": "ASC",
    }
    if since:
        params["modified"] = since

    resp = session.get(API_URL, params=params, timeout=60)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    batch: List[Dict[str, str]] = []
    for entry in root.findall("atom:entry", ns):
        ecli_el = entry.find("atom:id", ns)
        if ecli_el is None:
            continue
        published_el = entry.find("atom:updated", ns)
        if published_el is None:
            published_el = entry.find("atom:published", ns)
        if published_el is None:
            continue
        batch.append({"ecli": ecli_el.text, "published": published_el.text})

    return batch


def fetch_uitspraak(session: "requests.Session", ecli: str) -> str | None:
    """Return the plain-text body for ``ecli`` or ``None`` if missing."""

    resp = session.get(CONTENT_URL, params={"id": ecli}, timeout=60)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    ns = {"rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0"}
    el = root.find(".//rs:uitspraak", ns)
    if el is None:
        return None
    return ET.tostring(el, encoding="unicode", method="text").strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-items", type=int, default=500, help="Max XML records to fetch")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Crawl delay (s)")
    parser.add_argument("--resume", action="store_true", help="resume from saved offset")
    parser.add_argument("--start-offset", type=int, default=0, help="Initial ?start= offset")
    parser.add_argument("--start-date", type=str, default=None, help="YYYY-MM-DD to begin crawl")
    parser.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD to end crawl")
    args = parser.parse_args()

    state = load_checkpoint()
    token = os.getenv("HF_TOKEN")
    if not token:
        raise ValueError("HF_TOKEN not set")
    if login is None:
        raise ImportError(
            "The 'huggingface_hub' package is required to upload results. "
            "Install dependencies via 'pip install -r requirements.txt'."
        )
    login(token=token)
    session = _session()

    # adjust crawl delay
    global REQUEST_PAUSE
    REQUEST_PAUSE = args.delay

    start_date = datetime.fromisoformat(args.start_date).date() if args.start_date else None
    end_date = datetime.fromisoformat(args.end_date).date() if args.end_date else None

    offset = args.start_offset
    if args.resume:
        offset = state.get("current_offset", offset)
    else:
        state["current_offset"] = offset

    processed = 0
    while True:
        try:
            batch = fetch_ecli_page(session, state.get("last_published"), offset)
        except Exception as exc:  # noqa: BLE001
            logging.error("Page @ offset %s failed: %s", offset, exc)
            time.sleep(REQUEST_PAUSE * 5)
            continue

        if not batch:
            break

        records: List[Dict[str, str]] = []
        for item in batch:
            if processed >= args.max_items:
                break
            pub_dt = datetime.fromisoformat(item["published"]).date()
            if start_date and pub_dt < start_date:
                continue
            if end_date and pub_dt > end_date:
                continue
            content = fetch_uitspraak(session, item["ecli"])
            if not content:
                continue
            cleaned = scrub_names(content)
            records.append(
                {
                    "url": f"https://uitspraken.rechtspraak.nl/details?id={item['ecli']}",
                    "content": cleaned,
                    "source": "Rechtspraak",
                }
            )
            state["last_published"] = item["published"]
            processed += 1
            time.sleep(REQUEST_PAUSE)

        if records:
            logging.info("Uploading %d decisions", len(records))
            if Dataset is None:
                raise ImportError(
                    "The 'datasets' package is required to upload results. "
                    "Install dependencies via 'pip install -r requirements.txt'."
                )
            ds = Dataset.from_list(records)
            ds.push_to_hub(HF_REPO, token=token)

        offset += len(batch)
        state["current_offset"] = offset
        save_checkpoint(state)
        logging.info("Checkpoint saved @ offset %d", offset)

        if processed >= args.max_items or len(batch) < 1000:
            break

    state["current_offset"] = 0
    save_checkpoint(state)
    logging.info("Finished crawl")


if __name__ == "__main__":
    main()
