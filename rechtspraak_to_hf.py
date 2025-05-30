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

# ---------- helpers (compile once) ------------------------------------------
NAME_PREFIXES = r"(?:w\.g\.|\(w\.g\.\)|\(get\.\)|get\.|mr\.?|mrs\.?|mw\.?|dr\.?|drs\.?)\s*"

INITIALS      = r"(?:[A-Z]\.\s*){1,6}"                    # A. | A.B. | A. B. C. …

LOWER_PREFIXES = [
    "van der", "van den", "van", "den", "de", "der", "von",
    "ten", "ter", "te",           # e.g. ten Bosch
    r"'t", r"'s",                 # ’t Hoen, ’s-Gravesande
]
APOSTRO_PREFIX = r"[dDlL]'"

PREFIX_RE      = r"(?:{}|{})".format(
    "|".join(map(re.escape, LOWER_PREFIXES)),
    APOSTRO_PREFIX
)

SURNAME_CORE   = r"[A-Z][\wÀ-ÖØ-öø-ÿ'’\-]+"
SURNAME        = rf"(?:{PREFIX_RE}\s+|-)?{SURNAME_CORE}(?:[-\s]{SURNAME_CORE})*"

INLINE_NAME_RE = re.compile(rf"{NAME_PREFIXES}?{INITIALS}{SURNAME}", re.I)

# Lines that are always meta-data / signatures
HARD_SKIP_RE = re.compile(
    r"^(?:waarvan opgemaakt dit proces-verbaal|het gerechtshof verklaart het verzet ongegrond|"
    r"aldus vastgesteld|aldus gedaan|aldus gewezen|aldus uitgesproken|"
    r"gewezen door|gegeven door|uitgesproken in het openbaar|"
    r"ten overstaan van|in tegenwoordigheid van|meervoudige kamer|enkelvoudige kamer|"
    r"mr\.\s|de griffier|de voorzitter)",
    re.I,
)

# Lines that are nothing but surnames (no initials), often two or more
NAME_WORD      = rf"(?:{PREFIX_RE}\s+)?{SURNAME_CORE}"
SURNAME_ONLY_RE = re.compile(rf"^(?:{NAME_WORD})(?:\s+{NAME_WORD}){{1,6}}$", re.I)

# ---------- main function ----------------------------------------------------
def scrub_names(text: str) -> str:
    kept = []

    for raw in text.splitlines():
        line = raw.strip()

        # 1. Discard pure boiler-plate
        if HARD_SKIP_RE.match(line):
            continue

        # 2. Drop lines holding ≥2 inline names or ending in a name
        if len(INLINE_NAME_RE.findall(line)) >= 2 or INLINE_NAME_RE.search(line + " "):
            continue

        # 3. Drop lines that are only surnames without initials
        if SURNAME_ONLY_RE.match(line):
            continue

        # 4. Remove any stray inline name(s)
        line = INLINE_NAME_RE.sub("", line)

        # 5. Tidy up
        line = re.sub(r"\s+", " ", line).strip(" ,.–\u00A0")

        if line and len(line) > 2:
            kept.append(line)

    return "\n".join(kept).strip()

# Constants
HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
API_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

def fetch_eclis():
    print("[INFO] Fetching list of ECLIs from Rechtspraak API...")
    params = {
        "type": "uitspraak",
        "return": "DOC",
        "max": 500
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
