import os
from datasets import load_dataset, Dataset, Features, Value
from huggingface_hub import login

HF_REPO = "vGassen/dutch-court-cases-rechtspraak"
MAIN_SPLIT = "train"
INCOMING_SPLIT = "incoming"
MIN_EXPECTED_SIZE = 1_000  # safeguard: don't push if below this

def main():
    print("[INFO] Starting merge and push...")

    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        raise ValueError("HF_TOKEN not set")
    login(token=hf_token)

    try:
        # Stream the existing split to avoid loading everything into memory
        existing_stream = load_dataset(HF_REPO, split=MAIN_SPLIT, streaming=True)
        existing_urls = {item["url"] for item in existing_stream}
        existing_ds = load_dataset(HF_REPO, split=MAIN_SPLIT)
        print(f"[INFO] Loaded existing split '{MAIN_SPLIT}' with {len(existing_ds)} items.")
    except Exception as e:
        print(f"[ERROR] Could not load existing dataset: {e}")
        return

    try:
        incoming_ds = load_dataset(HF_REPO, split=INCOMING_SPLIT)
        incoming = [item for item in incoming_ds if item["url"] not in existing_urls]
        print(f"[INFO] Loaded {len(incoming)} new unique items from '{INCOMING_SPLIT}' split.")
    except Exception as e:
        print(f"[ERROR] Failed to load incoming data: {e}")
        incoming = []

    if not incoming:
        print("[INFO] No new items to merge. Skipping.")
        return

    # Merge and deduplicate
    from datasets import concatenate_datasets

    incoming_ds_unique = Dataset.from_list(incoming)
    merged_ds = concatenate_datasets([existing_ds, incoming_ds_unique])
    merged_ds = merged_ds.remove_columns([col for col in merged_ds.column_names if col not in {"url", "content", "source"}])
    merged_data_size = merged_ds.num_rows
    print(f"[INFO] Merged dataset has {merged_data_size} items.")

    # SAFETY CHECK: avoid accidental overwrite with small dataset
    if merged_data_size < MIN_EXPECTED_SIZE:
        raise RuntimeError(f"[ABORTED] Merged dataset only has {merged_data_size} items — too small to safely push.")

    # Push to main split
    print(f"[INFO] Uploading merged dataset to '{MAIN_SPLIT}'...")
    merged_ds.push_to_hub(HF_REPO, split=MAIN_SPLIT)

    # Clear incoming with correct schema
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
