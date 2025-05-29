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
        if any(keyword in l for keyword in ["de griffier", "mr.", "(getekend)", "griffier", "de voorzitter", "grififer"]):
            continue
        
        # Skip lines starting with mr. (case variations)
        if re.match(r"^.{0,5}mr\. ", l):
            continue
        
        # Skip lines with specific court signatures and common endings
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
        
        # Comprehensive Dutch name pattern in parentheses
        # Handles: (A.B.C. van der Surname), (A.B. Surname), (A. den Surname), etc.
        dutch_name_pattern = r"\([A-Z]\.?[A-Z]?\.?[A-Z]?\.?[A-Z]?\.?\s*(?:van\s+der\s+|van\s+den\s+|van\s+|den\s+|de\s+|der\s+)?[A-Z][a-z]+(?:-[A-Z][a-z]+)?\)"
        
        # Skip lines that contain two or more Dutch names in parentheses
        if len(re.findall(dutch_name_pattern, line)) >= 2:
            continue
        
        # Skip lines that end with Dutch names in parentheses (signature lines)
        if re.search(dutch_name_pattern + r"\s*" + dutch_name_pattern + r"\s*$", line):
            continue
        
        # Skip lines with "(c:XX)" pattern
        if re.search(r"\(c:\d+\)", l):
            continue
        
        # Remove inline Dutch names in parentheses but keep the rest of the line
        line_cleaned = re.sub(dutch_name_pattern, "", line)
        line_cleaned = re.sub(r"\(c:\d+\)", "", line_cleaned)
        
        # Clean up extra whitespace and punctuation left behind
        line_cleaned = re.sub(r'\s+', ' ', line_cleaned).strip()
        line_cleaned = re.sub(r'^\s*[,\.\-–]\s*', '', line_cleaned)  # Remove leading punctuation
        line_cleaned = re.sub(r'\s*[,\.\-–]\s*

# Keep these below the function
HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

def fetch_eclis():
    print("[INFO] Fetching list of ECLIs from Rechtspraak API...")
    params = {
        "type": "uitspraak",
        "return": "DOC",
        "max": 150  # Increased from 25 to 150
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
    main(), '', line_cleaned)  # Remove trailing punctuation
        
        # Only add line if it has meaningful content after cleaning
        if line_cleaned and not line_cleaned.isspace() and len(line_cleaned) > 2:
            clean_lines.append(line_cleaned)
    
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
        "max": 25  # Change this later to 1000 or more
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
