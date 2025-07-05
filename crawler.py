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
ALL_ECLIS_FILE = "all_rechtspraak_eclis.json"  # File to store all discovered ECLIs
CHECKPOINT_FILE = "processed_eclis.json"      # File to track processed ECLIs
JUDGES_FILE = "judge_names.json"              # List of judge names for scrubbing
BATCH_SIZE = 100                              # Number of records to process and upload in one batch
MAX_RECORDS_PER_RUN = 5000                    # Safety limit for a single execution of the script
REQUEST_DELAY_S = 1.0                         # Delay between API requests to be polite
MAX_RETRIES = 4                               # Number of retries for failed requests

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


def fetch_all_eclis():
    """
    Discovers all ECLIs from the Rechtspraak API and saves them to a file.
    This function should be run once to populate the initial list of cases.
    """
    logging.info("Starting ECLI discovery process. This may take a long time...")
    discovered_eclis = load_json_set(ALL_ECLIS_FILE)
    api_url = "https://data.rechtspraak.nl/uitspraken/zoeken"

    for doc_type in ("Uitspraak", "Conclusie"):
        logging.info(f"Discovering ECLIs for type '{doc_type}'...")
        start_index = 0

        while True:
            params = {
                "max": 1000,
                "from": start_index,
                "return": "META",
                "type": doc_type,
                "q": "*",  # required as of mid-2024
            }
            logging.info(f"Fetching {doc_type} ECLIs from index {start_index}...")
            try:
                response = get_with_retry(api_url, params=params)
                soup = BeautifulSoup(response.content, "xml")
                entries = soup.find_all("entry")

                if not entries:
                    logging.info("No more entries found. ECLI discovery complete.")
                    break

                batch_eclis = {entry.id.text for entry in entries if entry.id}
                newly_found = len(batch_eclis - discovered_eclis)

                if newly_found == 0 and len(entries) < 1000:
                    # If we get a partial page with no new ECLIs, we are likely at the end
                    logging.info("Reached a page with no new ECLIs. Concluding discovery.")
                    break

                discovered_eclis.update(batch_eclis)
                start_index += len(entries)  # Move to the next page

                # Save progress periodically
                if start_index % 5000 == 0:
                    save_json_set(discovered_eclis, ALL_ECLIS_FILE)
                    logging.info(
                        f"Saved progress. Total ECLIs discovered: {len(discovered_eclis)}"
                    )

                time.sleep(REQUEST_DELAY_S)
        
            except requests.RequestException as e:
                logging.error(f"A critical error occurred during ECLI discovery: {e}")
                break
            
    save_json_set(discovered_eclis, ALL_ECLIS_FILE)
    logging.info(f"Finished ECLI discovery. Total ECLIs found: {len(discovered_eclis)}")


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

    # Load ECLI lists
    all_eclis = load_json_set(ALL_ECLIS_FILE)
    if not all_eclis:
        logging.info(
            f"ECLI list file '{ALL_ECLIS_FILE}' is empty or not found. Running initial discovery..."
        )
        fetch_all_eclis()
        all_eclis = load_json_set(ALL_ECLIS_FILE)
        if not all_eclis:
            logging.error(
                f"Failed to populate '{ALL_ECLIS_FILE}' after discovery attempt."
            )
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
    
    for i in range(0, len(eclis_for_this_run), BATCH_SIZE):
        batch_eclis = eclis_for_this_run[i:i+BATCH_SIZE]
        records_to_upload = []
        
        logging.info(f"--- Processing Batch {i//BATCH_SIZE + 1} ---")
        for ecli in batch_eclis:
            record = process_ecli(ecli, judge_names)
            if record:
                records_to_upload.append(record)
            time.sleep(REQUEST_DELAY_S) # Be polite to the API

        if records_to_upload:
            try:
                logging.info(f"Uploading {len(records_to_upload)} new records to Hugging Face Hub...")
                batch_dataset = Dataset.from_list(records_to_upload)
                batch_dataset.push_to_hub(HF_DATASET_ID, private=False)
                
                # Update checkpoint file ONLY after successful upload
                processed_eclis.update(ecli for r in records_to_upload if (ecli := re.search(r'id=(ECLI:[^&]+)', r['URL'])) )
                save_json_set(processed_eclis, CHECKPOINT_FILE)
                logging.info("Upload and checkpoint successful.")
            except Exception as e:
                logging.error(f"Failed to push batch to Hugging Face Hub: {e}")
                logging.info("Aborting run to prevent data loss. Please check credentials and network.")
                return # Stop the run if a push fails
        else:
            logging.warning("No valid records were generated in this batch.")

    logging.info("Script run completed.")

if __name__ == "__main__":
    # To run the full scrape, you typically do two things:
    # 1. Discover all cases (run this once, or periodically to update)
    # fetch_all_eclis()
    
    # 2. Process the discovered cases (run this repeatedly until done)
    main()