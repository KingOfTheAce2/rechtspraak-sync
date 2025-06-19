"""
Merge Rechtspraak .jsonl fragments and push full dataset to Hugging Face Hub.
"""

import os
import json
from pathlib import Path
from typing import List, Dict, Set

import datasets  # pip install datasets
from tqdm import tqdm

# Settings
DATA_DIR = Path("data")
OUTPUT_PATH = DATA_DIR / "rechtspraak_merged.jsonl"
HUB_REPO = "vGassen/dutch-court-cases-rechtspraak" 


def find_jsonl_files(data_dir: Path) -> List[Path]:
    return sorted(data_dir.glob("*.jsonl"))


def merge_jsonl_files(files: List[Path]) -> List[Dict]:
    seen: Set[str] = set()
    merged: List[Dict] = []

    for file in files:
        with file.open(encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    ecli = record.get("ecli")
                    if ecli and ecli not in seen:
                        seen.add(ecli)
                        merged.append(record)
                except json.JSONDecodeError:
                    print(f"[warn] Skipped invalid JSON in: {file.name}")

    return sorted(merged, key=lambda r: r.get("ecli", ""))


def save_to_jsonl(records: List[Dict], out_file: Path) -> None:
    out_file.parent.mkdir(exist_ok=True)
    with out_file.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main():
    jsonl_files = find_jsonl_files(DATA_DIR)
    if not jsonl_files:
        print("⚠️  No .jsonl files found.")
        return

    print(f"🔍 Found {len(jsonl_files)} JSONL files to merge.")
    records = merge_jsonl_files(jsonl_files)
    print(f"✅ Merged {len(records):,} unique decisions.")

    save_to_jsonl(records, OUTPUT_PATH)
    print(f"📁 Saved merged file to: {OUTPUT_PATH}")

    ds = datasets.load_dataset("json", data_files=str(OUTPUT_PATH), split="train")
    ds.push_to_hub(HUB_REPO, private=True)
    print(f"🚀 Successfully pushed {len(ds):,} records to {HUB_REPO}")


if __name__ == "__main__":
    main()
