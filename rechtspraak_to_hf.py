import os
import requests
import xml.etree.ElementTree as ET
import json
from datasets import Dataset
from huggingface_hub import HfApi, HfFolder, login

# SET THIS TO YOUR HF DATASET REPO
HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

def fetch_eclis():
    params = {
        "type": "uitspraak",
        "return": "DOC",
        "max": 5  # increase this later
    }
    r = requests.get(API_URL, params=params)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    eclis = [entry.find("{http://www.w3.org/2005/Atom}id").text for entry in root.findall("{http://www.w3.org/2005/Atom}entry")]
    return eclis

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
        print(f"[ERROR] Failed to fetch {ecli}: {e}")
    return None

def save_to_jsonl(data, path="uitspraken.jsonl"):
    with open(path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

def main():
    # 🔐 Login to HuggingFace Hub using token from environment
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise ValueError("HF_TOKEN environment variable not set")
    login(token=hf_token)

    # 📥 Fetch data
    eclis = fetch_eclis()
    uitspraken =
