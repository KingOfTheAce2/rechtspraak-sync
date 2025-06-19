# crawl_rechtspraak.py
"""
Incremental crawler for Rechtspraak.nl Open‑Data
------------------------------------------------
  • Retrieves ECLI identifiers through the /uitspraken/zoeken Atom feed
  • Downloads the full XML decision for every ECLI
  • Scrubs personal names / signatures
  • Writes newline‑delimited JSON (.jsonl) and optionally pushes to HF Hub
"""

from __future__ import annotations
import datetime as dt
import json
import re
import time
from pathlib import Path
from typing import Iterable, List, Dict, Any, Optional

import requests
import feedparser          # pip install feedparser
from lxml import etree     # pip install lxml
from tqdm import tqdm      # pip install tqdm
import datasets            # pip install datasets

################################################################################
# CONSTANTS
################################################################################
BASE_SEARCH_URL = "https://data.rechtspraak.nl/uitspraken/zoeken"
BASE_CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"
USER_AGENT = (
    "RechtspraakCrawler/0.5 (orga@example.org) "
    "- respects server-load advice; one request per second."
)
MAX_PER_REQUEST = 1_000                          # hard limit set by API :contentReference[oaicite:0]{index=0}
REQUEST_PAUSE_SEC = 1.05                         # “Don’t hammer the server” :contentReference[oaicite:1]{index=1}
NAME_RE = re.compile(
    r"""
    \b
    (?:[A-Z][a‑z]{1,}\s+(?:van\s+|de\s+|den\s+|der\s+)?)*   # family prefixes
    [A-Z][a‑z]+                                             # surname
    (?:\s+[A-Z][a‑z]+)*                                     # optional second surname
    \b
    """,
    re.VERBOSE,
)
SIGNATURE_RE = re.compile(r"(?i)(?:Deze uitspraak is ondertekend).*?$", re.S)

###############################################################################
# HELPER FUNCTIONS
###############################################################################
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/xml"})
    s.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))
    return s


def _atom_page(
    session: requests.Session,
    **params: str,
) -> feedparser.FeedParserDict:
    resp = session.get(BASE_SEARCH_URL, params=params, timeout=60)
    resp.raise_for_status()
    return feedparser.parse(resp.text)


def _iter_eclis(
    session: requests.Session,
    modified_from: Optional[dt.datetime] = None,
) -> Iterable[str]:
    """
    Yields every ECLI that has *documents* (return=DOC) updated since
    `modified_from`.  Pagination handled via `from=` & `max=` parameters.
    """
    base_params = {"return": "DOC", "max": str(MAX_PER_REQUEST)}
    if modified_from:
        base_params["modified"] = modified_from.isoformat()

    offset = 0
    while True:
        params = {**base_params, "from": str(offset)}
        feed = _atom_page(session, **params)
        if not feed.entries:
            break

        for entry in feed.entries:
            # skip placeholder entries (deleted="ecli"/"doc")
            if getattr(entry, "deleted", False):
                continue
            yield entry.id

        offset += len(feed.entries)
        time.sleep(REQUEST_PAUSE_SEC)


def _download_xml(session: requests.Session, ecli: str) -> str:
    resp = session.get(BASE_CONTENT_URL, params={"id": ecli}, timeout=60)
    resp.raise_for_status()
    return resp.text


def _scrub_xml(xml_text: str) -> Dict[str, Any]:
    """
    - Strip personal names & judge signatures from textual nodes
    - Return dict with fields: ecli, text_clean, xml_raw
    """
    doc = etree.fromstring(xml_text.encode())
    ns = {"rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0"}

    # Fetch ECLI from metadata
    ecli = doc.xpath("string(//dcterms:identifier)", namespaces=doc.nsmap)

    # Collect all <para> nodes (covers bulk of the judgement text)
    paras = doc.xpath("//rs:para//text()", namespaces=ns)
    text = "\n".join(paras)

    # Scrub
    text = NAME_RE.sub("[PERSOON]", text)
    text = SIGNATURE_RE.sub("", text).strip()

    return {"ecli": ecli, "text_clean": text, "xml_raw": xml_text}


###############################################################################
# PUBLIC API
###############################################################################
def crawl(
    out_file: Path,
    modified_from: Optional[str | dt.datetime] = None,
    push_to_hub: Optional[str] = None,
) -> None:
    """
    Parameters
    ----------
    out_file : Path
        Destination .jsonl file.
    modified_from : str | datetime | None
        Only decisions created/changed after this moment.
        Accepts ISO‑date string ('2023-01-01') or dt.datetime.
        If None, *all* available decisions (~800 k) will be downloaded.
    push_to_hub : str | None
        If set, the dataset will be pushed to this HF repo (e.g. 'org/rs_nl').
    """
    if isinstance(modified_from, str):
        modified_from = dt.datetime.fromisoformat(modified_from)
    session = _session()
    out_file.parent.mkdir(parents=True, exist_ok=True)

    with out_file.open("w", encoding="utf‑8") as fh, tqdm(
        total=None, unit="doc", desc="Decisions"
    ) as pbar:
        for ecli in _iter_eclis(session, modified_from):
            try:
                xml = _download_xml(session, ecli)
                record = _scrub_xml(xml)
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                pbar.update()
            except Exception as exc:
                # log & continue
                print(f"[warn] {ecli}: {exc}")

            time.sleep(REQUEST_PAUSE_SEC)

    if push_to_hub:
        ds = datasets.load_dataset("json", data_files=str(out_file), split="train")
        ds.push_to_hub(push_to_hub, private=True)
        print(f"✅  Pushed {len(ds):,} records to {push_to_hub}")


###############################################################################
# CLI
###############################################################################
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Crawl Rechtspraak decisions")
    ap.add_argument(
        "--since",
        type=str,
        default=None,
        help="ISO date‑time (e.g. '2023-01-01T00:00:00') to crawl incrementally.",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("rechtspraak_decisions.jsonl"),
        help="Where to store the JSONL file.",
    )
    ap.add_argument(
        "--push",
        type=str,
        default=None,
        help="💡 Optional HuggingFace repo name to push the dataset.",
    )
    args = ap.parse_args()
    crawl(args.out, modified_from=args.since, push_to_hub=args.push)
