
import os
import requests
import xml.etree.ElementTree as ET
import json
from datasets import Dataset
from huggingface_hub import login
import re
import builtins
import sys

print = lambda *args, **kwargs: builtins.print(*args, **kwargs, flush=True)

def scrub_names(text):
    import json

    if not hasattr(scrub_names, "judge_names"):
        with open("judge_names.json", "r", encoding="utf-8") as f:
            scrub_names.judge_names = set(name.lower().strip() for name in json.load(f))

    lines = text.strip().split("\n")
    clean_lines = []

    for line in lines:
        l = line.lower()

        if any(judge in l for judge in scrub_names.judge_names):
            continue

        if any(keyword in l for keyword in ["de griffier", "mr.", "(getekend)", "griffier", "de voorzitter", "grififer"]):
            continue
        if re.match(r"^.{0,5}mr\. ", l):
            continue

        skip_patterns = [
            r"waarvan opgemaakt dit proces-verbaal.*\([A-Z]",
            r"het hof bevestigt.*\([A-Z]",
            r"aldus vastgesteld en uitgesproken.*\([A-Z]",
            r"veroordeelt.*proceskosten.*\([A-Z]",
            r"voormelde kamer.*\([A-Z]",
            r"de grififer.*het lid.*\([A-Z]",
            r"het lid van de voormelde kamer.*\([A-Z]"
        ]
        if any(re.search(pattern, l) for pattern in skip_patterns):
            continue

        dutch_name_pattern = r"\([A-Z]\.?[A-Z]?\.?[A-Z]?\.?[A-Z]?\.?\s*(?:van\s+der\s+|van\s+den\s+|van\s+|den\s+|de\s+|der\s+)?[A-Z][a-z]+(?:-[A-Z][a-z]+)?\)"
        if len(re.findall(dutch_name_pattern, line)) >= 2:
            continue
        if re.search(dutch_name_pattern + r"\s*" + dutch_name_pattern + r"\s*$", line):
            continue
        if re.search(r"\(c:\d+\)", l):
            continue

        line_cleaned = re.sub(dutch_name_pattern, "", line)
        line_cleaned = re.sub(r"\(c:\d+\)", "", line_cleaned)
        line_cleaned = re.sub(r'\s+', ' ', line_cleaned).strip()
        line_cleaned = re.sub(r'^\s*[,.\-–]\s*', '', line_cleaned)
        line_cleaned = re.sub(r',\s*$', '', line_cleaned)

        if line_cleaned and not line_cleaned.isspace() and len(line_cleaned) > 2:
            clean_lines.append(line_cleaned)

    return "\n".join(clean_lines).strip()

HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

def fetch_eclis():
    print("[INFO] Fetching list of ECLIs from Rechtspraak API...")
    params = {
        "type": "uitspraak",
        "return": "DOC",
        "max": 250
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
            content = scrub_names(content)
            uitspraken.append({
                "url": ecli,
                "content": content,
                "source": "Rechtspraak"
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
