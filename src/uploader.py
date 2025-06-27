# src/uploader.py

from huggingface_hub import HfApi
from pathlib import Path

from . import config

def upload_to_hf_hub(file_path: Path):
    """
    Uploads a single data file to the configured Hugging Face dataset repository.

    Args:
        file_path: The local path to the JSONL file to upload.
    """
    if not config.HF_DATASET_ID or "YourUsername" in config.HF_DATASET_ID:
        print("🤷 HF_DATASET_ID not configured. Skipping upload.")
        return

    print(f"🚀 Uploading {file_path.name} to Hugging Face Hub repository: {config.HF_DATASET_ID}...")
    api = HfApi()

    try:
        # Create repo if it doesn't exist
        api.create_repo(
            repo_id=config.HF_DATASET_ID,
            repo_type="dataset",
            private=config.HF_DATASET_PRIVATE,
            exist_ok=True,
        )

        # Upload the file
        api.upload_file(
            path_or_fileobj=str(file_path),
            path_in_repo=f"data/{file_path.name}",
            repo_id=config.HF_DATASET_ID,
            repo_type="dataset",
        )
        print(f"✅ Successfully uploaded {file_path.name} to the Hub.")
    except Exception as e:
        print(f"❌ Failed to upload to Hugging Face Hub. Error: {e}")
        print("Please ensure you are logged in via `huggingface-cli login` or have set the HUGGING_FACE_HUB_TOKEN environment variable.")
