# Dutch Court Decisions Dataset (Rechtspraak.nl)

This repository provides code to collect and clean Dutch court decisions published on [Rechtspraak.nl](http://www.rechtspraak.nl/Uitspraken-en-Registers/Uitspraken/Open-Data/Pages/default.aspx). The data is loaded from the official public API, processed to scrub personal names from the decision texts, and uploaded to the Hugging Face Hub as a machine learning–friendly dataset.

## About the Data

The Dutch Judiciary (Raad voor de Rechtspraak) provides a public RESTful web service to access:

- **ECLI metadata**: ECLI identifier, court name, case number, decision date, and citations.
- **Court decisions**: Full-text XML documents associated with ECLI identifiers.

To retrieve complete cases, two API calls are required:
1. Query the ECLI index to obtain identifiers based on filters (date, court, etc.).
2. Fetch the full decision content using those ECLIs.

### Coverage

- Over **800,000** full-text court decisions are available.
- Metadata (ECLI only) is available for **3+ million** cases dating back to **1913**.
- Full-text content is available for decisions from **1999** onwards.

### License

- **License**: Public domain
- **Access**: Public
- **Publisher**: Raad voor de Rechtspraak (Rijk)
- **Contact**: [kennissystemen@rechtspraak.nl](mailto:kennissystemen@rechtspraak.nl)
- **Code license**: see [LICENSE](LICENSE)

Source catalog: [data.overheid.nl](https://data.overheid.nl)

---

## Script Overview

The script performs the following:

1. **Fetches ECLI IDs** from the Rechtspraak search API.
2. **Downloads full decisions** in XML format.
3. **Scrubs personal names and signatures** to ensure anonymization using
   `judge_names.json` for judge names and regex patterns for lawyers.
4. **Saves results** in `.jsonl` format.
5. **Pushes the dataset** to Hugging Face using `datasets` library.

All processed texts are stripped of common Dutch judge signatures, clerical lines,
and Dutch name patterns (including typical lawyer forms like `Mr. X`).

---

## Usage

### Requirements

Install dependencies:

```bash
pip install -r requirements.txt
```

The requirements file includes `beautifulsoup4` which is needed for HTML/XML
parsing in the crawler.

### Running the crawler

The crawler respects the publisher's rate limit by sending one request per second and uses an ASCII-only `User-Agent` header to avoid encoding issues. The delay can be adjusted via the `REQUEST_DELAY_SEC` environment variable. Because over 800k decisions are available, long crawls can be resumed via the state file described below.

```bash
HF_TOKEN=your_token python crawler.py
```

On the first run the script checks for an existing `all_rechtspraak_eclis.json`
file containing the ECLI index. If it is missing, an initial discovery pass is
executed automatically to create it before processing any cases.

The crawler maintains a checkpoint so interrupted runs can resume automatically.
You can limit the number of items or adjust the API delay using environment
variables documented in the script.

### Resuming and sharding

The script keeps track of processed ECLI identifiers in `processed_eclis.json`.
If a run is interrupted simply execute `python crawler.py` again and it will
continue where it left off.

### Environment Variables

`BACKFILL_MAX_ITEMS` controls how many historical cases are processed in a
single run. By default it is set to `10000`.

`REQUEST_DELAY_SEC` defines the delay between API requests. It defaults to `1.0` second.

```bash
BACKFILL_MAX_ITEMS=5000 python -m src.main backfill
```

Historical backfills write results to `data/rechtspraak_backlog_<start>_<timestamp>.jsonl`.
Each run creates a new file so that uploads to the Hugging Face dataset do not
overwrite earlier batches.
