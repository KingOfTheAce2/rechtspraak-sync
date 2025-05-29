import os
import requests
import xml.etree.ElementTree as ET
import json
from datasets import Dataset
from huggingface_hub import login

# ✅ Force Python to flush output so GitHub shows it in logs
import sys
print = lambda *args, **kwargs: __builtins__.print(*args, **kwargs, flush=True)

HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

def fetch_eclis():
    print("[INFO] Fetching list of ECLIs from Rechtspraak API...")
    params = {
        "type": "uitspraak",
        "return": "DOC",
        "max": 5  # Change this later to 1000 or more
    }
    r = requests.get(API_URL, params=params)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    eclis = [entry.find("{http://www.w3.org/2005/Atom}id").text for entry in root.findall("{http://www.w3.org/2005/Atom}entry")]
    print(f"[INFO] Found {len(eclis)} ECLIs.")
    return eclis

def fetch_uitspraak(ecli):
    try:
        print(f"[INFO] Fetching uitspraak for {ecli}")
        r = requests.get(f"{CONTENT_URL}?id={ecli}")
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {"rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0"}
        uitspraak_el = root.find(".//rs:uitspraak", ns)
        if uitspraak_el is not None:
            text = ET.tostring(uitspraak_el, encoding="unicode", method="text").strip()
            print(f"[INFO] ✅ Got uitspraak for {ecli}")
            return text
        else:
            print(f"[WARN] ❌ No uitspraak found for {ecli}")
    except Exception as e:
        print(f"[ERROR] Failed to fetch uitspraak for {ecli}: {e}")
    return None

def save_to_jsonl(data, path="uitspraken.jsonl"):
    print(f"[INFO] Saving {len(data)} uitspraken to {path}")
    with open(path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

def main():
    print("[INFO] Starting Rechtspraak script...")
    
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise ValueError("HF_TOKEN environment variable not set")
    login(token=hf_token)
    print("[INFO] Logged in to HuggingFace Hub.")

    eclis = fetch_eclis()
    uitspraken = []

    for ecli in eclis:
        content = fetch_uitspraak(ecli)
        if content:
            uitspraken.append({
                "ecli": ecli,
                "uitspraak": content
            })

    print(f"[INFO] Total valid uitspraken collected: {len(uitspraken)}")

    if not uitspraken:
        print("[WARN] No uitspraken found. Skipping upload.")
        return

    save_to_jsonl(uitspraken)

    print("[INFO] Uploading dataset to HuggingFace...")
    dataset = Dataset.from_json("uitspraken.jsonl")
    dataset.push_to_hub(HF_REPO)
    print("[✅] Upload complete.")

if __name__ == "__main__":
    main()
