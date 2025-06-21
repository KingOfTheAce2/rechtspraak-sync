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
3. **Scrubs personal names and signatures** to ensure anonymization.
4. **Saves results** in `.jsonl` format.
5. **Pushes the dataset** to Hugging Face using `datasets` library.

All processed texts are stripped of common Dutch judge signatures, clerical lines, and Dutch name patterns in parentheses.

---

## Usage

### Requirements

Install dependencies:

```bash
pip install -r requirements.txt
```

### Running the crawler

The crawler respects the publisher's rate limit by sending one request per second and uses an ASCII-only `User-Agent` header to avoid encoding issues. Because over 800k decisions are available, long crawls can be resumed via the state file described below.

```bash
python crawl_rechtspraak.py \
  --since "$(date -u -d '1 hour ago' +'%Y-%m-%dT%H:%M:%S')" \
  --out data/rs_sync.jsonl \
  --push vGassen/dutch-court-cases-rechtspraak
```

A minimal example using an internal `checkpoint.json` for automatic resumption
and uploading is available via `rechtspraak_crawler.py`:

```bash
HF_TOKEN=your_token python rechtspraak_crawler.py
```

### Resuming and sharding

Use `--state-file` to log processed ECLI identifiers so that a subsequent run
can skip them:

```bash
python crawl_rechtspraak.py --state-file crawl.log --out data/part.jsonl
```

For large backfills the crawl can be split across multiple shards using
`--shard-index` and `--num-shards`:

```bash
# Two shards running in parallel
python crawl_rechtspraak.py --shard-index 0 --num-shards 2 --out data/s0.jsonl
python crawl_rechtspraak.py --shard-index 1 --num-shards 2 --out data/s1.jsonl
```
