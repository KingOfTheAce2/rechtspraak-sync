# src/main.py

import sys
import json
import datetime
from tqdm import tqdm

from . import api_client, parser, config, state_manager, uploader

def process_and_save(metadata_iterator, output_file_path):
    """
    Processes a stream of metadata, fetches full content, and saves to a file.
    """
    print(f"✍️ Writing data to {output_file_path}...")
    processed_count = 0
    with open(output_file_path, 'a', encoding='utf-8') as f:
        # Wrap the iterator with tqdm for a progress bar
        for entry in tqdm(metadata_iterator, desc="Processing cases", unit="case"):
            ecli_id = entry['id']
            if not ecli_id:
                continue

            # 1. Fetch full XML content
            xml_content = api_client.get_ruling_content(ecli_id)
            if not xml_content:
                continue

            # 2. Parse XML to get text
            full_text = parser.parse_ruling_xml(xml_content)

            # 3. Format and save
            record = {
                "URL": f"{config.DEEPLINK_URL_PREFIX}{ecli_id}",
                "Content": full_text,
                "Source": "Rechtspraak"
            }
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
            processed_count += 1
            
            # Save progress periodically for backfill
            if processed_count % config.API_MAX_RESULTS_PER_PAGE == 0 and 'backfill' in sys.argv:
                # State is based on start index, not processed count
                current_index = state_manager.load_state() + config.API_MAX_RESULTS_PER_PAGE
                state_manager.save_state(current_index)

    print(f"✅ Finished processing. Total cases saved in this run: {processed_count}")
    return processed_count

def backfill():
    """
    Runs the full historical data scraper.
    Resumes from the last known position.
    """
    print("🚀 Starting historical backfill...")
    start_index = state_manager.load_state()
    output_file = config.DATA_DIR / "rechtspraak_backlog.jsonl"

    metadata_stream = api_client.get_metadata_batch(start_from=start_index)
    
    total_processed = process_and_save(metadata_stream, output_file)

    if total_processed > 0:
        uploader.upload_to_hf_hub(output_file)

    print("🎉 Historical backfill complete.")

def update_daily():
    """
    Runs the daily updater to fetch recently modified cases.
    """
    print("🚀 Starting daily update...")
    # Get timestamp for 24 hours ago in ISO 8601 format (UTC)
    yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
    yesterday_iso = yesterday.isoformat()
    
    print(f"🔍 Fetching cases modified since {yesterday_iso}...")

    date_str = yesterday.strftime('%Y-%m-%d')
    output_file = config.DATA_DIR / f"rechtspraak_update_{date_str}.jsonl"

    metadata_stream = api_client.get_metadata_batch(modified_since=yesterday_iso)
    
    total_processed = process_and_save(metadata_stream, output_file)

    if total_processed > 0:
        uploader.upload_to_hf_hub(output_file)
    else:
        print("🤷 No new or updated cases found in the last 24 hours.")

    print("🎉 Daily update complete.")

def main():
    """Main entry point."""
    if len(sys.argv) < 2 or sys.argv[1] not in ['backfill', 'update_daily']:
        print("Usage: python -m src.main [backfill|update_daily]")
        sys.exit(1)

    command = sys.argv[1]
    if command == 'backfill':
        backfill()
    elif command == 'update_daily':
        update_daily()

if __name__ == "__main__":
    main()
