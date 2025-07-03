import os
import json
import time
import re
import requests
from pathlib import Path
from datasets import Dataset, load_dataset, concatenate_datasets
from huggingface_hub import HfApi, login

# CONFIG
HF_DATASET_ID = "vGassen/dutch-court-cases-rechtspraak"
CHECKPOINT_FILE = "processed_eclis.json"
BATCH_SIZE = 200
RUN_LIMIT = 2000
REQUEST_DELAY = 1.1
JUDGES_FILE = "judge_names.json"  # Your provided list

# --- 1. LOAD STATE ---
def load_processed_eclis():
    if not os.path.exists(CHECKPOINT_FILE): return set()
    try:
        with open(CHECKPOINT_FILE, "r") as f: return set(json.load(f))
    except Exception: return set()

def save_processed_eclis(processed):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(sorted(list(processed)), f, indent=2)

# --- 2. GET ECLI LIST FROM API ---
def fetch_eclis(start=0, limit=2000):
    eclis = []
    base = "https://data.rechtspraak.nl/uitspraken/zoeken"
    params = {
        "max": 1000,
        "from": start,
        "return": "DOC",
        "type": "uitspraak"
    }
    processed = 0
    while processed < limit:
        resp = requests.get(base, params=params, timeout=30)
        resp.raise_for_status()
        if "<entry>" not in resp.text: break
        for xml in resp.text.split("<entry>")[1:]:
            start_tag = xml.find("<id>")
            end_tag = xml.find("</id>")
            if start_tag != -1 and end_tag != -1:
                eclis.append(xml[start_tag+4:end_tag])
        processed += params["max"]
        params["from"] += params["max"]
        time.sleep(1)
    return eclis

# --- 3. GET CONTENT FOR EACH ECLI ---
def fetch_content(ecli):
    url = "https://data.rechtspraak.nl/uitspraken/content"
    resp = requests.get(url, params={"id": ecli}, timeout=30)
    if resp.status_code == 200: return resp.text
    return None

# --- 4. NAME SCRUBBING ---
def build_dutch_name_regex():
    # Simple Dutch first names list (for demo; extend for production)
    names = ["Jan", "Piet", "Kees", "Marie", "Anna", "Sophie"]
    return re.compile(r'\b(' + '|'.join(names) + r')\b', re.IGNORECASE)

def load_judge_names():
    with open(JUDGES_FILE, "r") as f:
        return set(json.load(f))

def scrub_names(text, judge_names, name_regex):
    for judge in judge_names:
        text = re.sub(rf'\b{re.escape(judge)}\b', "naam", text)
    text = name_regex.sub("naam", text)
    return text

# --- 5. PUSH TO HUGGINGFACE ---
def push_batch_to_hf(batch, existing):
    ds = Dataset.from_list(batch)
    combined = concatenate_datasets([existing, ds]) if existing else ds
    combined.push_to_hub(HF_DATASET_ID, private=False)
    return combined

# --- 6. MAIN PIPELINE ---
def main():
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        print("Set HF_TOKEN env variable!")
        return
    login(token=hf_token)
    try:
        existing = load_dataset(HF_DATASET_ID, split="train")
    except Exception:
        existing = None

    judge_names = load_judge_names()
    name_regex = build_dutch_name_regex()
    processed = load_processed_eclis()
    all_eclis = fetch_eclis(start=0, limit=RUN_LIMIT)
    eclis_to_process = [e for e in all_eclis if e not in processed]
    print(f"{len(eclis_to_process)} new ECLIs to process.")

    for i in range(0, len(eclis_to_process), BATCH_SIZE):
        batch = eclis_to_process[i:i+BATCH_SIZE]
        batch_data = []
        for ecli in batch:
            content = fetch_content(ecli)
            if not content or len(content) < 100: continue
            scrubbed = scrub_names(content, judge_names, name_regex)
            batch_data.append({
                "URL": f"https://deeplink.rechtspraak.nl/uitspraak?id={ecli}",
                "Content": scrubbed,
                "Source": "Rechtspraak"
            })
            time.sleep(REQUEST_DELAY)
        if batch_data:
            print(f"Pushing {len(batch_data)} records to HuggingFace...")
            existing = push_batch_to_hf(batch_data, existing)
            processed.update(batch)
            save_processed_eclis(processed)
        else:
            print("No records in batch.")
    print("Done.")

if __name__ == "__main__":
    main()
