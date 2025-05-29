import requests
import xml.etree.ElementTree as ET
import json
from datasets import Dataset
from huggingface_hub import HfApi, HfFolder

HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

def fetch_eclis():
    params = {
        "type": "uitspraak",
        "return": "DOC",
        "max": 5  # limit for demo; increase later
    }
    r = requests.get(API_URL, params=params)
    root = ET.fromstring(r.text)
    eclis = [entry.find("{http://www.w3.org/2005/Atom}id").text for entry in root.findall("{http://www.w3.org/2005/Atom}entry")]
    return eclis

def fetch_uitspraak(ecli):
    r = requests.get(f"{CONTENT_URL}?id={ecli}")
    if r.status_code != 200:
        return None

    try:
        root = ET.fromstring(r.content)
        ns = {"rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0"}
        uitspraak_el = root.find(".//rs:uitspraak", ns)
        if uitspraak_el is not None:
            return ET.tostring(uitspraak_el, encoding="unicode", method="text").strip()
    except:
        return None

def save_to_jsonl(data, path="uitspraken.jsonl"):
    with open(path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")

def main():
    eclis = fetch_eclis()
    uitspraken = []

    for ecli in eclis:
        content = fetch_uitspraak(ecli)
        if content:
            uitspraken.append({
                "ecli": ecli,
                "uitspraak": content
            })

    save_to_jsonl(uitspraken)

    # Upload to HuggingFace
    dataset = Dataset.from_json("uitspraken.jsonl")
    dataset.push_to_hub(HF_REPO)

if __name__ == "__main__":
    main()
