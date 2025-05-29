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
pip install datasets huggingface_hub requests
