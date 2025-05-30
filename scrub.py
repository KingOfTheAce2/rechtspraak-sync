import os
import json
from pathlib import Path

JUDGE_LIST_PATH = "judge_names.json"
DATA_DIR = "data"

def load_judge_names():
    with open(JUDGE_LIST_PATH, "r", encoding="utf-8") as f:
        return set(json.load(f))

def scrub_case(case, judge_names):
    for field in ["judges", "body", "summary"]:  # adjust field names as needed
        if field in case:
            for name in judge_names:
                case[field] = case[field].replace(name, "[REDACTED]")
    return case

def main():
    judge_names = load_judge_names()
    for file_path in Path(DATA_DIR).glob("*.json"):
        with open(file_path, "r", encoding="utf-8") as f:
            case = json.load(f)

        scrubbed = scrub_case(case, judge_names)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(scrubbed, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
