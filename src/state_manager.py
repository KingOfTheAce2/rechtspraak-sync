# src/state_manager.py

import json
import datetime
from . import config

def save_state(last_processed_index: int):
    """Saves the last successfully processed 'from' index to the state file."""
    state = {
        "last_processed_index": last_processed_index,
        "last_updated": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    with open(config.STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)
    print(f"✅ State saved. Resuming from index: {last_processed_index}")

def load_state() -> int:
    """Loads the last processed 'from' index from the state file."""
    if not config.STATE_FILE.exists():
        return 0
    try:
        with open(config.STATE_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
            last_index = state.get("last_processed_index", 0)
            print(f"▶️ Resuming from last saved index: {last_index}")
            return last_index
    except (json.JSONDecodeError, IOError) as e:
        print(f"⚠️ Could not read state file, starting from scratch. Error: {e}")
        return 0
