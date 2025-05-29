import os
import requests
import xml.etree.ElementTree as ET
import json
from datasets import Dataset
from huggingface_hub import login
import re
import builtins

# ✅ Force Python to flush output so GitHub shows it in logs
import sys
print = lambda *args, **kwargs: builtins.print(*args, **kwargs, flush=True)

# 👉 Add the scrubber here, after all imports:
def scrub_names(text):
    lines = text.strip().split("\n")
    clean_lines = []
    for line in lines:
        l = line.lower()
        
        # Skip lines with common court official phrases
        if any(keyword in l for keyword in ["de griffier", "mr.", "(getekend)", "griffier", "de voorzitter"]):
            continue
        
        # Skip lines starting with mr. (case variations)
        if re.match(r"^.{0,5}mr\. ", l):
            continue
        
        # Skip lines with names in parentheses (like court officials)
        if re.search(r"\([A-Z][a-z]*\.[A-Z][a-z]*\.[A-Z][a-z]*\.\s*[A-Z][a-z]*\)", line):
            continue
        
        # Skip lines with initials and surnames in parentheses (broader pattern)
        if re.search(r"\([A-Z]\.[A-Z]\.[A-Z]\.[A-Z]\.?\s*[A-Z][a-z]+\)", line):
            continue
        
        # Skip lines with process-verbaal signatures
        if "proces-verbaal" in l and "(" in line and ")" in line:
            continue
        
        # Skip lines with "(c:XX)" pattern followed by names
        if re.search(r"\(c:\d+\)", l):
            continue
        
        # Skip lines that are mostly initials and names (common signature patterns)
        if re.search(r"^\s*\([A-Z]\.?[A-Z]\.?[A-Z]?\.?\s*[A-Z][a-z]+\)\s*\([A-Z]\.?[A-Z]\.?[A-Z]?\.?\s*[A-Z][a-z]+\)\s*$", line):
            continue
        
        # Remove inline names in parentheses but keep the rest of the line
        line_cleaned = re.sub(r"\([A-Z]\.[A-Z]\.?[A-Z]?\.?\s*[A-Z][a-z]+\)", "", line)
        line_cleaned = re.sub(r"\(c:\d+\)", "", line_cleaned)
        
        # Only add line if it has meaningful content after cleaning
        if line_cleaned.strip():
            clean_lines.append(line_cleaned.strip())
    
    return "\n".join(clean_lines).strip()

# Keep these below the function
HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

def fetch_eclis():
    print("[INFO] Fetching list of ECLIs from Rechtspraak API...")
    params = {
        "type": "uitspraak",
        "return": "DOC",
        "max": 50  # Change this later to 1000 or more
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
