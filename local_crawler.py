import sqlite3
import requests
from lxml import etree
from datasets import Dataset
from huggingface_hub import HfApi
import os
import re # Import the regular expression module
from typing import List, Dict, Optional, Set
import subprocess # Required for convert_pdf_to_text
import json # Required for loading JSON file

DB_PATH = "progress.sqlite3"
# GLOBAL_BATCH_LIMIT is removed to ensure all data is processed.

# Define XML Namespaces
ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"
RECHTSPRAAK_RS_NAMESPACE = "http://www.rechtspraak.nl/schema/rechtspraak-1.0"
RECHTSPRAAK_ECLI_NAMESPACE = "https://e-justice.europa.eu/ecli"
RECHTSPRAAK_PSI_NAMESPACE = "http://psi.rechtspraak.nl/"
RECHTSPRAAK_DCTERMS_NAMESPACE = "http://purl.org/dc/terms/"
RECHTSPRAAK_RDF_NAMESPACE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

NAMESPACES = {
    'atom': ATOM_NAMESPACE,
    'rs': RECHTSPRAAK_RS_NAMESPACE,
    'ecli': RECHTSPRAAK_ECLI_NAMESPACE,
    'psi': RECHTSPRAAK_PSI_NAMESPACE,
    'dcterms': RECHTSPRAAK_DCTERMS_NAMESPACE,
    'rdf': RECHTSPRAAK_RDF_NAMESPACE,
}


def get_skiptoken(category: str) -> int:
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "CREATE TABLE IF NOT EXISTS progress(category TEXT PRIMARY KEY, skiptoken INTEGER)"
    )
    cur = con.execute("SELECT skiptoken FROM progress WHERE category=?", (category,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else -1


def save_skiptoken(category: str, skiptoken: int) -> None:
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "REPLACE INTO progress(category, skiptoken) VALUES(?,?)", (category, skiptoken)
    )
    con.commit()
    con.close()


def convert_pdf_to_text(pdf_content: bytes) -> str:
    """Converts PDF bytes to plain text using pdftotext."""
    try:
        process = subprocess.run(
            ["pdftotext", "-q", "-", "-"], # -q for quiet, - for stdin, - for stdout
            input=pdf_content, # pdf_content is bytes, which is correct for stdin of external process
            capture_output=True,
            check=True, # Raise an exception for non-zero exit codes
        )
        return process.stdout.decode('utf-8', errors='ignore')
    except subprocess.CalledProcessError as e:
        error_output = e.stderr.decode('utf-8', errors='ignore') if e.stderr else ""
        print(f"Error converting PDF to text: {e.cmd} returned {e.returncode} with output: {error_output}")
        return "" # Return empty string on conversion error
    except Exception as e:
        print(f"Unexpected error during PDF conversion: {e}")
        return ""


def load_judge_names(filepath: str) -> Set[str]:
    """Loads a set of judge names from a JSON file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            names = json.load(f)
            # Assuming the JSON file contains a list of strings
            if isinstance(names, list) and all(isinstance(n, str) for n in names):
                return set(names)
            else:
                print(f"Warning: {filepath} does not contain a list of strings. Returning empty set.")
                return set()
    except FileNotFoundError:
        print(f"Error: Judge names file not found at {filepath}. Returning empty set.")
        return set()
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {filepath}. Returning empty set.")
        return set()
    except Exception as e:
        print(f"An unexpected error occurred while loading judge names: {e}. Returning empty set.")
        return set()


def anonymize_text(content: str, judge_names: Set[str]) -> str:
    """Anonymizes judge and lawyer names in the court case text."""
    # Replace judge names using regex with word boundaries for precision
    for name in judge_names:
        # Use re.escape to handle special characters in names
        content = re.sub(rf'\b{re.escape(name)}\b', "[naam]", content, flags=re.IGNORECASE)

    # Replace lawyer names (e.g., Mr. Lastname, mr. van der Laan)
    # This regex is designed to capture common Dutch lawyer titles and name formats.
    # It looks for "Mr", "mr", "meester" optionally followed by a dot,
    # then one or more spaces, and then a capitalized word followed by
    # optional connecting words (van, de, der, etc.) and another capitalized word,
    # or just a single capitalized word.
    lawyer_pattern = r'\b(Mr|mr|meester)\.?\s+([A-Z][\w\'-]+(?:(?:\s+(?:van|de|der|den|ter|ten|d\'))?\s+[A-Z][\w\'-]+)*|[A-Z][\w\'-]+)'
    content = re.sub(lawyer_pattern, "[naam]", content)
    return content


def fetch_all_docs(query_category: str, judge_names: Set[str]) -> List[Dict[str, str]]:
    all_docs: List[Dict[str, str]] = []
    current_from_token: Optional[int] = get_skiptoken(query_category)

    initial_api_url = "http://data.rechtspraak.nl/uitspraken/zoeken"
    
    batch_size = 1000 # Max results per page as per documentation

    while True:
        params = {
            "return": "DOC", # Crucial to get documents, not just metadata
            "max": batch_size
        }
        # Add 'from' parameter for pagination
        if current_from_token is not None and current_from_token >= 0:
            params["from"] = current_from_token
        
        print(f"--- Fetching API: {initial_api_url} with params: {params} ---")
        try:
            resp = requests.get(initial_api_url, params=params, timeout=60) # Increased timeout
            resp.raise_for_status()
            print(f"API Response Status: {resp.status_code}")
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")
            break

        root = etree.fromstring(resp.content)
        
        entries = root.findall("atom:entry", NAMESPACES)
        entry_count = len(entries)
        print(f"API returned {entry_count} entries in this batch.")
        
        # If no entries were returned, it might indicate the end of the feed
        if not entries:
            print("No entries found in the current batch. End of feed or no new data.")
            break

        next_link_found = False
        
        for entry in entries:
            entry_id = entry.find("atom:id", NAMESPACES).text if entry.find("atom:id", NAMESPACES) is not None else "N/A"
            
            # Check for deleted entries (Rechtspraak API uses 'deleted' attribute on entry)
            is_deleted = entry.get("deleted") == "doc" or entry.get("deleted") == "ecli"
            if is_deleted:
                print(f"Skipping entry {entry_id}: marked as deleted.")
                continue

            # In Rechtspraak API, you construct the document URL directly from the ECLI (entry_id)
            document_url = f"https://data.rechtspraak.nl/uitspraken/{entry_id}"
            
            fetched_content = ""
            
            print(f"Processing entry {entry_id} (Document URL: {document_url})")
            
            try:
                doc_resp = requests.get(document_url, timeout=60) # Increased timeout for document fetch
                doc_resp.raise_for_status()
                print(f"Document fetch status for {document_url}: {doc_resp.status_code}, content_length: {len(doc_resp.content)} bytes.")

                actual_content_type = doc_resp.headers.get('Content-Type', '').split(';')[0].strip().lower()

                if actual_content_type == "application/pdf":
                    print(f"Attempting to convert PDF for {document_url} to text...")
                    fetched_content = convert_pdf_to_text(doc_resp.content)
                    if not fetched_content.strip():
                        print(f"Warning: PDF {document_url} conversion yielded empty/whitespace text. Skipping.")
                        continue
                    print(f"PDF converted successfully. Text length: {len(fetched_content)} characters.")
                elif actual_content_type == "application/xml" or actual_content_type.startswith("text/"):
                    doc_root = etree.fromstring(doc_resp.content)
                    
                    content_elements = doc_root.xpath("//rs:uitspraak//rs:para | //rs:conclusie//rs:para", namespaces=NAMESPACES)
                    
                    if content_elements:
                        fetched_content = "\n".join([p.text for p in content_elements if p.text is not None])
                    else:
                        print(f"Warning: No <rs:para> content found directly within <rs:uitspraak> or <rs:conclusie> for {document_url}. Trying atom:content.")
                        atom_content_element = entry.find("atom:content", NAMESPACES)
                        if atom_content_element is not None and atom_content_element.text is not None:
                            try:
                                nested_xml_root = etree.fromstring(atom_content_element.text.encode('utf-8'))
                                nested_content_elements = nested_xml_root.xpath("//rs:uitspraak//rs:para | //rs:conclusie//rs:para", namespaces=NAMESPACES)
                                fetched_content = "\n".join([p.text for p in nested_content_elements if p.text is not None])
                                if not fetched_content.strip():
                                    fetched_content = atom_content_element.text
                            except etree.XMLSyntaxError:
                                fetched_content = atom_content_element.text
                else:
                    print(f"Skipping unrecognized content type: {actual_content_type} for {document_url}.")
                    continue
                
                if not fetched_content.strip():
                    print(f"Warning: No significant text extracted from {document_url}. Skipping.")
                    continue

                # --- Anonymization Step ---
                # Only anonymize if judge names are loaded
                if judge_names: 
                    fetched_content = anonymize_text(fetched_content, judge_names)
                else:
                    print("Warning: No judge names loaded for anonymization. Content will not be scrubbed.")
                # --- End Anonymization Step ---

                all_docs.append({"URL": document_url, "content": fetched_content, "Source": "Rechtspraak"})

            except requests.exceptions.RequestException as e:
                print(f"Error fetching document '{document_url}': {e}. Skipping this document.")
                continue
            except etree.XMLSyntaxError as e:
                print(f"Error parsing document XML for '{document_url}': {e}. Skipping this document.")
                continue

        feed_next_link = root.find("atom:link[@rel='next']", NAMESPACES)
        if feed_next_link is not None:
            href = feed_next_link.get("href")
            if "from=" in href:
                try:
                    current_from_token = int(href.split("from=")[1].split("&")[0])
                    next_link_found = True
                    print(f"Found next 'from' token at feed level: {current_from_token}")
                except ValueError:
                    print(f"Could not parse 'from' token from URL: {href}")
                    pass
        
        if not next_link_found or entry_count == 0:
            print("No 'next' link found or no more entries. End of feed.")
            break
            
    print(f"Collected total of {len(all_docs)} documents.")
    if current_from_token is not None:
        save_skiptoken(query_category, current_from_token)
        print(f"Saved 'from' token for category '{query_category}': {current_from_token}")
    else:
        print("No 'from' token to save for this run.")

    return all_docs


def push_to_hf(docs: List[Dict[str, str]], repo_id: str) -> None:
    if not docs:
        print("No documents to push to Hugging Face.")
        return
    
    print(f"--- Preparing to push {len(docs)} documents to Hugging Face repo: {repo_id} ---")
    api = HfApi()
    
    try:
        local_parquet_path = "data.parquet"
        
        ds = Dataset.from_list(docs)
        ds.to_parquet(local_parquet_path) 
        print(f"Successfully saved {len(docs)} documents to {local_parquet_path}.")

        api.upload_file(
            path_or_fileobj=local_parquet_path,
            path_in_repo="data/latest.parquet", # Changed destination path
            repo_id=repo_id,
            repo_type="dataset",
        )
        print(f"Successfully uploaded data to {repo_id}!")
    except Exception as e:
        print(f"Error uploading to Hugging Face: {e}")


if __name__ == "__main__":
    category = "rechtspraak_judgments" 
    
    # --- Load Judge Names for Anonymization ---
    judge_names_filepath = "juge_names.json" 
    # IMPORTANT: Create a file named 'juge_names.json' in the same directory as this script.
    # It should contain a JSON array of strings, where each string is a name to be scrubbed.
    # Example content for 'juge_names.json':
    # [
    #   "John Doe",
    #   "Jane Smith",
    #   "P. Jansen"
    # ]
    loaded_judge_names = load_judge_names(judge_names_filepath)
    if not loaded_judge_names:
        print(f"Warning: Anonymization may not be effective as no judge names were loaded from {judge_names_filepath}.")
    # --- End Load Judge Names ---

    hf_repo_id = os.getenv("HF_REPO_ID", "vGassen/dutch-court-cases-rechtspraak") 
    print(f"Hugging Face Repository ID: {hf_repo_id}")
    
    # Pass the loaded judge_names to the fetch_all_docs function
    batch = fetch_all_docs(category, loaded_judge_names)
    push_to_hf(batch, hf_repo_id)