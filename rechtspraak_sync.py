# STEP 1: create a persistent checkpoint file
# This will track what we've already processed so we can continue later
import os
import json
import time
import requests
import xml.etree.ElementTree as ET
from datasets import Dataset, DatasetDict
from huggingface_hub import login
import re

CHECKPOINT_FILE = "checkpoint.json"
HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

# Load judge name list
with open("judge_names.json", "r", encoding="utf-8") as f:
    JUDGE_NAMES = json.load(f)

# Load checkpoint if available
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_published": None, "done_eclis": [], "empty_runs": 0}

# Save checkpoint

def save_checkpoint(state):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

# Balanced name scrubber

def scrub_names(text):
    name_pattern = r"((?:[A-Z]\.? ?){1,4}(?:van den |van der |van |de |den )?[A-Z][a-z]+(?:-[A-Z][a-z]+)?)"
    signature_markers = ["(get.)", "w.g.", "(getekend)"]

    lines = text.splitlines()
    clean_lines = []

    for line in lines:
        for full_name in JUDGE_NAMES:
            parts = full_name.split()
            if len(parts) < 2:
                continue

            # Split prefix and name
            prefix = " ".join(p for p in parts if "." not in p and p.lower() in {"mr.", "dhr.", "mw.", "prof.", "mr.drs.", "mr.dr.", "dr.", "drs."})
            name_part = full_name.replace(prefix, "").strip()

            if name_part in line:
                line = line.replace(name_part, "[NAAM]")

        # Regex fallback for initials + surname
        line = re.sub(r"(\(?)(?:[A-Z]\.? ?){1,4}(?:van den |van der |van |de |den )?[A-Z][a-z]+(?:-[A-Z][a-z]+)?(\)?)", r"\1[NAAM]\2", line)

        for marker in signature_markers:
            line = line.replace(marker, "")

        if re.search(r"aldus vastgesteld|in tegenwoordigheid van|deze uitspraak|getekend", line, re.IGNORECASE):
            continue

        line = re.sub(r"\s+", " ", line).strip()
        line = re.sub(r"^[,.:;-]+", "", line).strip()

        if line:
            clean_lines.append(line)

    return "\n".join(clean_lines).strip()

# Fetch and walk through paginated Atom feeds
def fetch_ecli_batch(after_timestamp=None, max_pages=5):
    collected = []
    page_url = API_URL + "?type=uitspraak&return=DOC&max=100"
    if after_timestamp:
        page_url += f"&published-min={after_timestamp}"

    pages = 0
    while page_url and pages < max_pages:
        r = requests.get(page_url)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        for entry in root.findall("atom:entry", ns):
            ecli_el = entry.find("atom:id", ns)
            published_el = entry.find("atom:published", ns)
            updated_el = entry.find("atom:updated", ns)

            if ecli_el is None:
                print("[WARN] Skipping entry with no ECLI.")
                continue

            ecli = ecli_el.text
            published = (published_el or updated_el).text if (published_el or updated_el) is not None else None

            if published is None:
                print(f"[WARN] Skipping entry {ecli} — no published or updated timestamp.")
                continue

            collected.append({"ecli": ecli, "published": published})

        next_link = root.find("atom:link[@rel='next']", ns)
        page_url = next_link.attrib['href'] if next_link is not None else None
        pages += 1
        time.sleep(1)

    return collected

# Fetch uitspraak XML by ECLI
def fetch_uitspraak(ecli):
    try:
        r = requests.get(f"{CONTENT_URL}?id={ecli}")
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {"rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0"}
        uitspraak_el = root.find(".//rs:uitspraak", ns)
        if uitspraak_el is not None:
            return ET.tostring(uitspraak_el, encoding="unicode", method="text").strip()
    except Exception as e:
        print(f"[ERROR] Failed to fetch content for {ecli}: {e}")
    return None

# Main

def main():
    checkpoint = load_checkpoint()
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise ValueError("HF_TOKEN not set")
    login(token=hf_token)

    print("[INFO] Fetching new ECLIs...")
    new_eclis = fetch_ecli_batch(after_timestamp=checkpoint["last_published"], max_pages=10)
    print(f"[INFO] Got {len(new_eclis)} ECLIs")

    uitspraken = []
    skipped = 0
    failed = 0

    for item in new_eclis:
        ecli = item["ecli"]
        published = item["published"]

        if ecli in checkpoint["done_eclis"]:
            skipped += 1
            continue

        content = fetch_uitspraak(ecli)
        if not content:
            failed += 1
            continue

        content = scrub_names(content)
        uitspraken.append({
            "url": f"https://uitspraken.rechtspraak.nl/details?id={ecli}",
            "content": content,
            "source": "Rechtspraak"
        })

        checkpoint["done_eclis"].append(ecli)
        checkpoint["last_published"] = published
        time.sleep(1)

    print(f"[INFO] Skipped already processed: {skipped}")
    print(f"[INFO] Failed to fetch: {failed}")
    print(f"[INFO] Collected new: {len(uitspraken)}")

    if uitspraken:
        print(f"[INFO] Uploading {len(uitspraken)} to HuggingFace")
        dataset = Dataset.from_list(uitspraken)
        dataset.push_to_hub(HF_REPO)
        checkpoint["empty_runs"] = 0
    else:
        checkpoint["empty_runs"] += 1
        if checkpoint["empty_runs"] >= 5:
            print("[WARN] ⚠️ No new uitspraken found in the last 5 runs.")
        else:
            print("[INFO] No new uitspraken to upload.")

    save_checkpoint(checkpoint)
    print("[INFO] Checkpoint updated.")

if __name__ == "__main__":
    main()
