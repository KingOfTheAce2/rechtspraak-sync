import os
import json
from pathlib import Path
from huggingface_hub import HfApi, HfFolder

# Load judge names
def load_judge_names(path="judge_names.json"):
    with open(path, "r", encoding="utf-8") as f:
        return set(json.load(f))

# Scrub judge names from known fields
def scrub_case(case, judge_names):
    for field in ["judges", "body", "summary"]:
        if field in case and isinstance(case[field], str):
            for name in judge_names:
                case[field] = case[field].replace(name, "[REDACTED]")
    return case

# Scrub all JSON files in current directory
def scrub_root_dir(judge_names_path="judge_names.json"):
    judge_names = load_judge_names(judge_names_path)
    for file_path in Path(".").glob("*.json"):
        if file_path.name == "judge_names.json":
            continue  # Skip the judge list itself
        with open(file_path, "r", encoding="utf-8") as f:
            case = json.load(f)

        scrubbed = scrub_case(case, judge_names)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(scrubbed, f, ensure_ascii=False, indent=2)

# Upload current repo content to Hugging Face
def upload_to_huggingface(repo_id="vGassen/dutch-court-cases-rechtspraak"):
    token = HfFolder.get_token()
    api = HfApi()
    api.upload_folder(
        folder_path=".",
        repo_id=repo_id,
        path_in_repo="",
        repo_type="dataset",
        token=token,
        exclude=[".git/*", ".github/*", "__pycache__/*", "*.py", "requirements.txt"]
    )

if __name__ == "__main__":
    scrub_root_dir()
    upload_to_huggingface()
