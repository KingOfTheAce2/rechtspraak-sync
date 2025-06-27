# src/api_client.py

import time
import requests
from typing import Iterator, Dict, Any, Optional

from . import config

def get_metadata_batch(start_from: int = 0, modified_since: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    """
    Fetches a paginated stream of metadata entries from the /zoeken endpoint.

    Args:
        start_from: The starting index for pagination ('from' parameter).
        modified_since: ISO 8601 datetime string to filter for recent updates.

    Yields:
        A dictionary for each court case entry found.
    """
    current_pos = start_from

    while True:
        params = {
            'max': config.API_MAX_RESULTS_PER_PAGE,
            'from': current_pos,
            'return': 'DOC',  # Return only ECLI's with documents
            'type': 'uitspraak'
        }
        if modified_since:
            params['modified'] = modified_since

        try:
            response = requests.get(config.BASE_API_URL, params=params, timeout=60)
            response.raise_for_status()

            feed = response.text
            if not feed or "<entry>" not in feed:
                print("🏁 No more entries found. Concluding metadata fetch.")
                break

            # Use string manipulation for performance on simple Atom feed
            entries = feed.split('<entry>')
            if len(entries) <= 1:
                print("🏁 Reached the end of the results.")
                break

            for entry_xml in entries[1:]: # Skip the header part
                ecli_id_start = entry_xml.find('<id>')
                if ecli_id_start == -1: continue
                ecli_id_end = entry_xml.find('</id>', ecli_id_start)
                ecli_id = entry_xml[ecli_id_start+4:ecli_id_end]
                yield {'id': ecli_id}

            current_pos += config.API_MAX_RESULTS_PER_PAGE

        except requests.exceptions.RequestException as e:
            print(f"❌ Network error while fetching metadata: {e}. Retrying in 30s...")
            time.sleep(30)
            continue


def get_ruling_content(ecli_id: str, max_retries: int = 3) -> Optional[str]:
    """
    Fetches the full XML content for a single ECLI.

    Args:
        ecli_id: The European Case Law Identifier.
        max_retries: Number of times to retry on failure.

    Returns:
        The raw XML content as a string, or None if it fails.
    """
    params = {'id': ecli_id}
    for attempt in range(max_retries):
        try:
            response = requests.get(config.CONTENT_API_URL, params=params, timeout=60)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            print(f"❌ Failed to fetch content for {ecli_id} (Attempt {attempt + 1}/{max_retries}). "
                  f"Error: {e}. Retrying in {wait}s...")
            time.sleep(wait)
    print(f"🚨 Giving up on {ecli_id} after {max_retries} attempts.")
    return None
