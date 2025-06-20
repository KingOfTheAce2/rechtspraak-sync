#!/usr/bin/env python3
"""
Merge all Rechtspraak crawl shards found with --pattern and
push the resulting dataset to the Hugging Face Hub.

Example
-------
python merge_and_push.py \
       --pattern "data/rs_*.jsonl" \
       --repo   "vGassen/dutch-court-cases-rechtspraak" \
       --token  $HF_TOKEN

If ``--repo`` is omitted, ``vGassen/dutch-court-cases-rechtspraak`` will be
used or the value of the ``HF_REPO`` environment variable if set.
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from typing import List

from datasets import Dataset, DatasetDict, concatenate_datasets, load_dataset
from huggingface_hub import HfApi, HfFolder


DEFAULT_REPO = "vGassen/dutch-court-cases-rechtspraak"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def merge_jsonl(pattern: str) -> Dataset:
    """Read every JSON‑Lines file that matches *pattern* and concatenate them
    into one in‑memory `datasets.Dataset`."""
    files: List[str] = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matched {pattern!r}")

    parts = [
        load_dataset("json", data_files=f, split="train", streaming=False)
        for f in files
    ]
    return concatenate_datasets(parts)


def push_dataset(ds: Dataset, repo_id: str, token: str | None) -> None:
    """Create the Hub repo if it does not yet exist and upload *ds*.
    We wrap the single split in a `DatasetDict` because
    `push_to_hub()` is implemented on `DatasetDict` and on `Dataset`
    (newer versions) but the dict variant is the most future‑proof."""
    api = HfApi(token=token or HfFolder.get_token())
    api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)

    dsdict = DatasetDict({"train": ds})
    # keep individual Parquet files below 500 MB so we never hit
    # the 100 MB Git‑LFS hard limit imposed by the Hub.
    dsdict.push_to_hub(
        repo_id,
        token=api.token,
        max_shard_size="500MB",  # safe default
    )
    print(
        f"✅  Uploaded {len(ds):,} rows → https://huggingface.co/datasets/{repo_id}",
        file=sys.stderr,
    )


# --------------------------------------------------------------------------- #
# cli
# --------------------------------------------------------------------------- #
def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--pattern", default="data/*.jsonl", help="Glob of crawl shards")
    p.add_argument(
        "--repo",
        default=os.getenv("HF_REPO", DEFAULT_REPO),
        help="Hub repo name (default: %(default)s)",
    )
    p.add_argument(
        "--token",
        help="HF access token. "
        "If omitted, the value in HF_TOKEN / HUGGINGFACE_TOKEN or the"
        " token saved by `huggingface-cli login` is used.",
    )
    args = p.parse_args()

    ds = merge_jsonl(args.pattern)
    push_dataset(
        ds,
        repo_id=args.repo,
        token=args.token
        or os.getenv("HF_TOKEN")
        or os.getenv("HUGGINGFACE_TOKEN"),
    )


if __name__ == "__main__":
    main()
