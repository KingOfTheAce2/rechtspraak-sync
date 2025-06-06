import os
from datasets import load_dataset, Dataset, Features, Value
from huggingface_hub import login

HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
MAIN_SPLIT = "train"
INCOMING_SPLIT = "incoming"

def main():
    print("[INFO] Starting merge and push...")

    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise ValueError("HF_TOKEN not set")
    login(token=hf_token)

    try:
        existing = load_dataset(HF_REPO, split=MAIN_SPLIT).to_list()
        print(f"[INFO] Loaded {len(existing)} existing items from '{MAIN_SPLIT}' split.")
    except Exception as e:
        print(f"[WARN] Could not load existing dataset. Starting fresh: {e}")
        existing = []

    try:
        incoming = load_dataset(HF_REPO, split=INCOMING_SPLIT).to_list()
        print(f"[INFO] Loaded {len(incoming)} new items from '{INCOMING_SPLIT}' split.")
    except Exception as e:
        print(f"[ERROR] Failed to load incoming data: {e}")
        incoming = []

    if not incoming:
        print("[INFO] No new items to merge.")
        return

    # Merge & deduplicate based on URL
    merged_dict = {item["url"]: item for item in existing + incoming}
    merged_data = list(merged_dict.values())
    print(f"[INFO] Merged dataset has {len(merged_data)} unique items.")

    # Push merged data to 'train'
    print(f"[INFO] Uploading merged dataset to '{MAIN_SPLIT}'...")
    Dataset.from_list(merged_data).push_to_hub(HF_REPO, split=MAIN_SPLIT)

    # Clear 'incoming' split using explicit schema
    print(f"[INFO] Clearing '{INCOMING_SPLIT}' split...")
    features = Features({
        "url": Value("string"),
        "content": Value("string"),
        "source": Value("string")
    })
    empty_dataset = Dataset.from_dict({"url": [], "content": [], "source": []}, features=features)
    empty_dataset.push_to_hub(HF_REPO, split=INCOMING_SPLIT)

    print("[✅] Merge and upload complete.")

if __name__ == "__main__":
    main()
