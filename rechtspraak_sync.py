# STEP 1: create a persistent checkpoint file
# This will track what we've already processed so we can continue later
import os
import json
import time
import requests
import xml.etree.ElementTree as ET
from datasets import Dataset, DatasetDict
from huggingface_hub import login

CHECKPOINT_FILE = "checkpoint.json"
HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

# Load checkpoint if available
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_published": None, "done_eclis": []}

# Save checkpoint

def save_checkpoint(state):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

# Scrubbing logic goes here (reuse or import your function)
def scrub_names(text):
    # Minimal stub for now
    return text

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
            ecli = entry.find("atom:id", ns).text
            published = entry.find("atom:published", ns).text
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
    new_eclis = fetch_ecli_batch(after_timestamp=checkpoint["last_published"], max_pages=20)
    print(f"[INFO] Got {len(new_eclis)} ECLIs")

    uitspraken = []
    for item in new_eclis:
        ecli = item["ecli"]
        published = item["published"]

        if ecli in checkpoint["done_eclis"]:
            continue

        content = fetch_uitspraak(ecli)
        if not content:
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

    if uitspraken:
        print(f"[INFO] Uploading {len(uitspraken)} to HuggingFace")
        dataset = Dataset.from_list(uitspraken)
        dataset.push_to_hub(HF_REPO)
    else:
        print("[INFO] No new uitspraken to upload.")

    save_checkpoint(checkpoint)
    print("[INFO] Checkpoint updated.")

if __name__ == "__main__":
    main()
