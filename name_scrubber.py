# name_scrubber.py
"""Light‑weight name scrubbing utility for Dutch court rulings.

This module does **one thing only**: replace personal names with the token
`[NAAM]`.

Design choices  
--------------  
* **Judge list only** – Hard list of ~8 k official judge names is loaded from
  *judge_names.json* and removed **everywhere** it appears.
* **Role‑based scrubbing** – Any name that immediately follows common role
  words (e.g. *gedaagde, eiser, gemachtigde*) is removed.  Only the name is
  replaced – the role word itself is kept so the legal context remains.
* **No fuzzy heuristics** – We _do not_ guess at other capitalised words.
  This prevents over‑scrubbing of months, articles, etc.
* **No side effects** – The file contains **no** I/O, networking or CLI
  helpers.  Import → call `scrub_names(text)` → done.
"""
from __future__ import annotations

import re
import json
from pathlib import Path
from typing import List, Tuple, Dict
from dataclasses import dataclass

__all__ = ["UltraNameScrubber", "scrub_names"]

_TOKEN = "[NAAM]"
_TITLE_STRIP = re.compile(
    r"^\s*(dhr|mw|mr|mrs|prof\.?|drs?|dr\.?|ing\.?|ir\.?|jonkheer|baron)\.?\s*",
    re.I,
)

# ----------------------------------------------------------------------------
#  Statistics helper (simple but useful when integrating)
# ----------------------------------------------------------------------------
@dataclass
class Stats:
    replaced: int = 0  # total individual names replaced

# ----------------------------------------------------------------------------
#  Core scrubber
# ----------------------------------------------------------------------------
class UltraNameScrubber:
    """Minimal but accurate replacement of personal names in Dutch rulings."""

    # ---------------------------------------------------------------------
    def __init__(self, judge_file: Path | str, token: str = _TOKEN) -> None:
        self.token = token
        self.stats = Stats()
        self._load_judges(Path(judge_file))
        self._init_regexes()

    # ---------------------------------------------------------------------
    #  Load & pre‑compile judge list
    # ---------------------------------------------------------------------
    def _load_judges(self, fp: Path) -> None:  # → self._judge_chunk_rx: List[re.Pattern]
        with fp.open(encoding="utf-8") as f:
            raw = json.load(f)

        cleaned = [_TITLE_STRIP.sub("", n).strip() for n in raw]
        self._judge_chunk_rx: List[re.Pattern] = []
        chunk: List[str] = []
        for name in cleaned:
            chunk.append(re.escape(name))
            if len(chunk) == 400:  # regex engine safety
                self._judge_chunk_rx.append(re.compile(r"|".join(chunk), re.I))
                chunk = []
        if chunk:
            self._judge_chunk_rx.append(re.compile(r"|".join(chunk), re.I))

    # ---------------------------------------------------------------------
    #  Build role‑based pattern once
    # ---------------------------------------------------------------------
    def _init_regexes(self) -> None:
        initials = r"(?:[A-Z]\.\s*){0,6}"  # up to ‘A.B.C. ’
        prefixes = [
            "van der", "van den", "van de", "van het", "van", "den", "de", "der",
            "von", "ten", "ter", "te", "'t", "'s", "op de", "op den", "op het",
            "in de", "in den", "in het", "in", "tot", "uit de", "uit den", "uit het",
            "bij de", "aan de", "voor de",
        ]
        prefix_re = rf"(?:{'|'.join(map(re.escape, prefixes))})\s+"
        surname_core = r"[A-ZÀ-ÖØ-öø-ÿ'][\wÀ-ÖØ-öø-ÿ'’\-]{1,24}"
        surname = rf"{prefix_re}?{surname_core}(?:[-\s]{surname_core})*"

        role_words = [
            "gedaagde", "eiser", "eiseres", "eisers", "geïntimeerde", "appellant",
            "appellante", "gemachtigde", "verzoeker", "verzoekster", "verweerder",
            "verweerster", "betrokkene", "advocaat", "raadsman", "raadsvrouw",
        ]
        # Capture role word (group 1) & following name (group 2)
        self._role_rx = re.compile(
            rf"\b({'|'.join(role_words)})\b\s*(?:de\s+)?(?:heer|dame|mr\.?|mw\.?)?\s*"
            rf"{initials}?({surname})",
            re.I,
        )

    # ---------------------------------------------------------------------
    #  Public API
    # ---------------------------------------------------------------------
    def scrub_names(self, text: str) -> str:  # returns cleaned text
        self.stats = Stats()

        # 1️⃣  Replace hard‑listed judges first (anywhere in text)
        for rx in self._judge_chunk_rx:
            text, n = rx.subn(self.token, text)
            self.stats.replaced += n

        # 2️⃣  Replace names that *immediately follow* a role word
        def _role_sub(match: re.Match) -> str:
            self.stats.replaced += 1
            return f"{match.group(1)} {self.token}"

        text = self._role_rx.sub(_role_sub, text)

        return text

    # Optional – quick metrics when integrating
    def report(self) -> Dict[str, int]:
        return self.stats.__dict__.copy()

# ---------------------------------------------------------------------------
#  Thin convenience wrapper for one‑off usage
# ---------------------------------------------------------------------------

def scrub_names(text: str) -> str:
    """Factory helper to scrub text with default judge list in same folder."""
    judge_path = Path(__file__).with_name("judge_names.json")
    return UltraNameScrubber(judge_path).scrub_names(text)
