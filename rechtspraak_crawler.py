import os
import json
import time
import re
import requests
import xml.etree.ElementTree as ET
from datasets import Dataset
from huggingface_hub import login

CHECKPOINT_FILE = "checkpoint.json"
HF_REPO = "vGassen/dutch-public-domain-texts"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"
USER_AGENT = "RechtspraakCrawler/1.0 (example@example.org)"
REQUEST_PAUSE = 1.0

# Load judge name list relative to this file
NAMES_PATH = os.path.join(os.path.dirname(__file__), "judge_names.json")
with open(NAMES_PATH, "r", encoding="utf-8") as f:
    JUDGE_NAMES = json.load(f)


def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_published": None, "done_eclis": [], "empty_runs": 0}


def save_checkpoint(state: dict) -> None:
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/xml"})
    s.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))
    return s


def scrub_names(text: str) -> str:
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

        line = re.sub(
            r"(\(?)(?:[A-Z]\.? ?){1,4}(?:van den |van der |van |de |den )?[A-Z][a-z]+(?:-[A-Z][a-z]+)?(\)?)",
            r"\1[NAAM]\2",
            line,
        )
        for marker in signature_markers:
            line = line.replace(marker, "")

        if re.search(r"aldus vastgesteld|in tegenwoordigheid van|deze uitspraak|getekend", line, re.IGNORECASE):
            continue

        line = re.sub(r"\s+", " ", line).strip()
        line = re.sub(r"^[,.:;-]+", "", line).strip()

        if line:
            clean_lines.append(line)

    return "\n".join(clean_lines).strip()


def fetch_ecli_batch(session: requests.Session, after_timestamp: str | None = None, max_pages: int = 5) -> list[dict]:
    collected: list[dict] = []
    page_url = f"{API_URL}?type=uitspraak&return=DOC&max=100"
    if after_timestamp:
        page_url += f"&modified-min={after_timestamp}"

    pages = 0
    while page_url and pages < max_pages:
        resp = session.get(page_url, timeout=60)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        for entry in root.findall("atom:entry", ns):
            ecli_el = entry.find("atom:id", ns)
            if ecli_el is None:
                continue
            published_el = entry.find("atom:updated", ns) or entry.find("atom:published", ns)
            if published_el is None:
                continue
            collected.append({"ecli": ecli_el.text, "published": published_el.text})

        next_link = root.find("atom:link[@rel='next']", ns)
        page_url = next_link.attrib.get("href") if next_link is not None else None
        pages += 1
        time.sleep(REQUEST_PAUSE)

    return collected


def fetch_uitspraak(session: requests.Session, ecli: str) -> str | None:
    resp = session.get(CONTENT_URL, params={"id": ecli}, timeout=60)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    ns = {"rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0"}
    el = root.find(".//rs:uitspraak", ns)
    if el is None:
        return None
    return ET.tostring(el, encoding="unicode", method="text").strip()


def main() -> None:
    state = load_checkpoint()
    token = os.getenv("HF_TOKEN")
    if not token:
        raise ValueError("HF_TOKEN not set")
    login(token=token)
    session = _session()

    print("[INFO] Fetching new ECLIs...")
    batch = fetch_ecli_batch(session, after_timestamp=state["last_published"], max_pages=10)
    print(f"[INFO] Got {len(batch)} ECLIs")

    uitspraken: list[dict] = []
    for item in batch:
        ecli = item["ecli"]
        if ecli in state["done_eclis"]:
            continue
        content = fetch_uitspraak(session, ecli)
        if not content:
            continue
        cleaned = scrub_names(content)
        uitspraken.append({
            "url": f"https://uitspraken.rechtspraak.nl/details?id={ecli}",
            "content": cleaned,
            "source": "Rechtspraak",
        })
        state["done_eclis"].append(ecli)
        state["last_published"] = item["published"]
        time.sleep(REQUEST_PAUSE)

    if uitspraken:
        print(f"[INFO] Uploading {len(uitspraken)} decisions to HuggingFace")
        ds = Dataset.from_list(uitspraken)
        ds.push_to_hub(HF_REPO)
        state["empty_runs"] = 0
    else:
        state["empty_runs"] = state.get("empty_runs", 0) + 1
        if state["empty_runs"] >= 5:
            print("[WARN] No new uitspraken found in the last 5 runs.")
        else:
            print("[INFO] No new uitspraken to upload.")

    save_checkpoint(state)
    print("[INFO] Checkpoint updated.")


if __name__ == "__main__":
    main()
