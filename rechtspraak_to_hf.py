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
    
    # Pattern for names in parentheses, now supporting diacritics
    dutch_name_paren = r"\([A-Z]\.?[A-Z]?\.?[A-Z]?\.?[A-Z]?\.?\s*(?:van\s+der\s+|van\s+den\s+|van\s+|den\s+|de\s+|der\s+)?[A-Z][a-zA-Zéèëöüäïáàíóúñçß\-']+\)"
    
    # Pattern for names after roles like gemachtigde, raadsman, etc.
    role_name = r"(gemachtigde|raadsman|advocaat|mr\.?)\s*[:\-]?\s*(de heer|mevrouw|mr\.)?\s*[A-Z]\.?[A-Z]?\.?[A-Z]?\.?\s*(?:van\s+der\s+|van\s+den\s+|van\s+|den\s+|de\s+|der\s+)?[A-Z][a-zA-Zéèëöüäïáàíóúñçß\-']+"

    # Common line-start signatures
    skip_prefixes = [
        r"waarvan opgemaakt dit proces-verbaal",
        r"het gerechtshof verklaart het verzet ongegrond",
        r"aldus vastgesteld.*",
        r"ten overstaan van.*",
        r"mr\..*",
        r"de griffier.*",
        r"de voorzitter.*",
    ]

    for line in lines:
        l = line.lower().strip()

        # Hard skip: full line matches
        if any(re.match(pat, l) for pat in skip_prefixes):
            continue
        
        # Hard skip: line has 2+ Dutch-style names in parentheses
        if len(re.findall(dutch_name_paren, line)) >= 2:
            continue
        
        # Hard skip: line *ends* with a known name in parentheses
        if re.search(f"{dutch_name_paren}\s*$", line):
            continue

        # Hard skip: contains name after a title/role (e.g. gemachtigde)
        if re.search(role_name, line, re.IGNORECASE):
            continue
        
        # Soft replace: clean inline name in parentheses
        line_cleaned = re.sub(dutch_name_paren, "", line)

        # Soft replace: clean inline role-based name
        line_cleaned = re.sub(role_name, "", line_cleaned, flags=re.IGNORECASE)

        # Clean artifacts
        line_cleaned = re.sub(r"\s+", " ", line_cleaned).strip()
        line_cleaned = re.sub(r"^\s*[,\.\-–]\s*", "", line_cleaned)
        line_cleaned = re.sub(r"\s*[,\.\-–]\s*$", "", line_cleaned)

        # Only keep lines with meaningful content
        if line_cleaned and len(line_cleaned) > 2:
            clean_lines.append(line_cleaned)

    return "\n".join(clean_lines).strip()

# Constants
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
