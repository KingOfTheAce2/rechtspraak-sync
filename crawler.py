import os
import json
import time
import re
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from datasets import Dataset
from huggingface_hub import login

# --- CONFIGURATION ---
HF_DATASET_ID = "vGassen/dutch-court-cases-rechtspraak"
ALL_ECLIS_FILE = "all_rechtspraak_eclis.json"      # File to store all discovered ECLIs
CHECKPOINT_FILE = "processed_eclis.json"            # File to track processed ECLIs
JUDGES_FILE = "judge_names.json"                    # List of judge names for scrubbing
DISCOVERY_STATE_FILE = "discovery_state.json"        # Track discovery progress
BATCH_INFO_FILE = "batch_state.json"                 # Track uploaded batch count
BATCH_SIZE = 100                                      # Number of records per upload batch
MAX_RECORDS_PER_RUN = 5000                            # Safety limit for a single execution of the script
REQUEST_DELAY_S = 1.0                                 # Delay between API requests
MAX_RETRIES = 4                                       # Number of retries for failed requests
DISCOVERY_BATCH_LIMIT = int(os.getenv("DISCOVERY_BATCH_LIMIT", "50000"))

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# --- 1. STATE AND ECLI MANAGEMENT ---

def load_json_set(filepath: str) -> set:
    """Loads a set of items from a JSON file."""
    if not os.path.exists(filepath):
        return set()
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except (json.JSONDecodeError, IOError) as e:
        logging.error(f"Could not read or parse {filepath}: {e}")
        return set()

def save_json_set(data_set: set, filepath: str):
    """Saves a set of items to a JSON file."""
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(sorted(list(data_set)), f, indent=2)
    except IOError as e:
        logging.error(f"Could not write to {filepath}: {e}")

def get_with_retry(url: str, params: dict = None, attempts: int = MAX_RETRIES) -> requests.Response:
    """Performs a GET request with exponential backoff retry logic."""
    for i in range(attempts):
        try:
            response = requests.get(url, params=params, timeout=45)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if i == attempts - 1:
                logging.error(f"Final attempt failed for URL {url}. Error: {e}")
                raise
            backoff_time = (2 ** i) + (0.1 * os.getpid()) # Add jitter
            logging.warning(f"Request failed (attempt {i+1}/{attempts}): {e}. Retrying in {backoff_time:.2f}s...")
            time.sleep(backoff_time)
    raise requests.RequestException("All retry attempts failed.")


DISCOVERY_DONE = -1


def load_discovery_state() -> dict:
    """Loads discovery progress for each document type."""
    if not os.path.exists(DISCOVERY_STATE_FILE):
        return {"Uitspraak": 0, "Conclusie": 0}
    try:
        with open(DISCOVERY_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Could not load {DISCOVERY_STATE_FILE}: {e}")
        return {"Uitspraak": 0, "Conclusie": 0}


def save_discovery_state(state: dict):
    """Persists discovery progress to disk."""
    try:
        with open(DISCOVERY_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logging.error(f"Could not write {DISCOVERY_STATE_FILE}: {e}")


def load_batch_number() -> int:
    """Returns the last uploaded batch number."""
    if not os.path.exists(BATCH_INFO_FILE):
        return 0
    try:
        with open(BATCH_INFO_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return int(data.get("last_batch", 0))
    except Exception:
        return 0


def save_batch_number(num: int):
    """Persists the last uploaded batch number."""
    try:
        with open(BATCH_INFO_FILE, "w", encoding="utf-8") as f:
            json.dump({"last_batch": num}, f, indent=2)
    except Exception as e:
        logging.error(f"Could not write {BATCH_INFO_FILE}: {e}")


def discover_eclis_batch(limit: int = DISCOVERY_BATCH_LIMIT) -> int:
    """Fetches a limited batch of new ECLIs and updates the discovery files."""
    discovered_eclis = load_json_set(ALL_ECLIS_FILE)
    state = load_discovery_state()
    api_url = "https://data.rechtspraak.nl/uitspraken/zoeken"

    total_new = 0
    for doc_type in ("Uitspraak", "Conclusie"):
        start_index = state.get(doc_type, 0)
        if start_index == DISCOVERY_DONE or total_new >= limit:
            continue
        logging.info(f"Discovering {doc_type} ECLIs starting at index {start_index}...")

        while total_new < limit:
            params = {
                "max": 1000,
                "from": start_index,
                "type": doc_type,
                "q": "*",
            }
            logging.info(f"Fetching {doc_type} ECLIs from index {start_index}...")
            try:
                response = get_with_retry(api_url, params=params)
                soup = BeautifulSoup(response.content, "xml")
                entries = soup.find_all("entry")

                if not entries:
                    state[doc_type] = DISCOVERY_DONE
                    logging.info(f"No more entries for {doc_type}.")
                    break

                batch_eclis = {entry.id.text.replace('%', ':') for entry in entries if entry.id}
                newly_found = len(batch_eclis - discovered_eclis)
                discovered_eclis.update(batch_eclis)
                total_new += newly_found

                start_index += len(entries)
                state[doc_type] = start_index

                if len(entries) < 1000:
                    state[doc_type] = DISCOVERY_DONE
                    break

                if total_new >= limit:
                    break

                time.sleep(REQUEST_DELAY_S)
            except requests.RequestException as e:
                logging.error(f"Error during discovery: {e}")
                break

    save_json_set(discovered_eclis, ALL_ECLIS_FILE)
    save_discovery_state(state)
    logging.info(f"Discovered {total_new} new ECLIs in this batch.")
    return total_new


# --- 2. DATA PROCESSING AND ANONYMIZATION ---

def anonymize_text(content: str, judge_names: set) -> str:
    """Anonymizes judge and lawyer names in the court case text."""
    # Replace judge names using regex with word boundaries for precision
    for name in judge_names:
        content = re.sub(rf'\b{re.escape(name)}\b', "[naam]", content, flags=re.IGNORECASE)

    # Replace lawyer names (e.g., Mr. Lastname, mr. van der Laan)
    lawyer_pattern = r'\b(Mr|mr|meester)\.?\s+([A-Z][\w\'-]+(?:\s+(?:van|de|der|den|ter|ten|d\'))?\s+[A-Z][\w\'-]+|[A-Z][\w\'-]+)'
    content = re.sub(lawyer_pattern, "[naam]", content)
    return content

def process_ecli(ecli: str, judge_names: set) -> dict | None:
    """Fetches, parses, and anonymizes the content for a single ECLI."""
    content_url = "https://data.rechtspraak.nl/uitspraak"
    try:
        ecli = ecli.replace('%', ':')
        response = get_with_retry(content_url, params={"id": ecli})
        soup = BeautifulSoup(response.content, "xml")

        content_tag = soup.find("uitspraak")
        if not content_tag or len(content_tag.get_text(strip=True)) < 100:
            logging.warning(f"No meaningful content found for {ecli}. Skipping.")
            return None

        content = content_tag.get_text(separator="\n", strip=True)
        anonymized_content = anonymize_text(content, judge_names)

        # Find the official public URL
        link_tag = soup.find("atom:link", {"rel": "alternate", "type": "text/html"})
        url = link_tag["href"] if link_tag else f"https://uitspraken.rechtspraak.nl/#!/details?id={ecli}"

        return {"URL": url, "content": anonymized_content, "source": "Rechtspraak"}

    except requests.RequestException:
        logging.error(f"Could not fetch content for {ecli} after multiple retries.")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {ecli}: {e}")
        return None


# --- 3. MAIN EXECUTION PIPELINE ---

def main():
    """Main pipeline to process and upload court cases."""
    # Authenticate with Hugging Face
    hf_token = os.getenv("HF_TOKEN")
    if not hf_token:
        logging.error("HF_TOKEN environment variable not set. Please set it to your Hugging Face write token.")
        return
    login(token=hf_token)

    # Load judge names for anonymization
    judge_names = load_json_set(JUDGES_FILE)
    if not judge_names:
        logging.warning(f"Could not load judge names from {JUDGES_FILE}. Proceeding without this anonymization step.")

    # Ensure we have an up-to-date ECLI index
    discovered = discover_eclis_batch()
    if discovered:
        logging.info(f"Discovered {discovered} ECLIs in this run.")

    all_eclis = load_json_set(ALL_ECLIS_FILE)
    if not all_eclis:
        logging.error(
            f"'{ALL_ECLIS_FILE}' is missing or empty even after discovery.")
        return

    processed_eclis = load_json_set(CHECKPOINT_FILE)
    eclis_to_process = sorted(list(all_eclis - processed_eclis))

    if not eclis_to_process:
        logging.info("No new ECLIs to process. All discovered cases are already in the checkpoint file.")
        return

    logging.info(f"Found {len(all_eclis)} total ECLIs.")
    logging.info(f"{len(processed_eclis)} ECLIs already processed.")
    logging.info(f"Starting new run with {len(eclis_to_process)} ECLIs remaining.")

    eclis_for_this_run = eclis_to_process[:MAX_RECORDS_PER_RUN]
    batch_number = load_batch_number()
    
    for i in range(0, len(eclis_for_this_run), BATCH_SIZE):
        batch_eclis = eclis_for_this_run[i:i+BATCH_SIZE]
        records_to_upload = []
        
        batch_number += 1
        logging.info(f"--- Processing Batch {batch_number} ---")
        for ecli in batch_eclis:
            record = process_ecli(ecli, judge_names)
            if record:
                record["batch"] = batch_number
                records_to_upload.append(record)
            time.sleep(REQUEST_DELAY_S) # Be polite to the API

        if records_to_upload:
            try:
                logging.info(f"Uploading {len(records_to_upload)} new records to Hugging Face Hub...")
                batch_dataset = Dataset.from_list(records_to_upload)
                batch_dataset.push_to_hub(HF_DATASET_ID, private=False)
                
                # Update checkpoint file ONLY after successful upload
                processed_eclis.update(
                    ecli for r in records_to_upload if (ecli := re.search(r"id=(ECLI:[^&]+)", r["URL"]))
                )
                save_json_set(processed_eclis, CHECKPOINT_FILE)
                save_batch_number(batch_number)
                logging.info("Upload and checkpoint successful.")
            except Exception as e:
                logging.error(f"Failed to push batch to Hugging Face Hub: {e}")
                logging.info("Aborting run to prevent data loss. Please check credentials and network.")
                return # Stop the run if a push fails
        else:
            logging.warning("No valid records were generated in this batch.")

    logging.info("Script run completed.")

if __name__ == "__main__":
    # Running the script will discover new ECLIs and then process them.
    main()
