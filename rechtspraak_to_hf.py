import os
import sys
import re
import json
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict, Tuple
from dataclasses import dataclass
from difflib import SequenceMatcher

# Flush print output for CI logs
print = lambda *args, **kwargs: builtins.print(*args, **kwargs, flush=True)

# ---------------------------------------------------------------------------
#  Statistics helper
# ---------------------------------------------------------------------------
@dataclass
class ScrubbingStats:
    lines_processed: int = 0
    lines_removed:   int = 0
    names_found:     int = 0
    patterns_matched: Dict[str, int] = None

    def __post_init__(self):
        if self.patterns_matched is None:
            self.patterns_matched = {}

# ---------------------------------------------------------------------------
#  Ultra-scrubber
# ---------------------------------------------------------------------------
class UltraNameScrubber:
    """Scrubs judge names and role-based personal names from Dutch rulings."""

    def __init__(self, judge_names_file: Path, token: str = "[NAAM]") -> None:
        self.token = token
        self.stats = ScrubbingStats()
        self._load_judge_names(judge_names_file)
        self._init_patterns()

    # ---------- judge list ---------------------------------------------------
    def _load_judge_names(self, fp: Path) -> None:
        with fp.open(encoding="utf-8") as f:
            raw = json.load(f)

        strip_title = re.compile(
            r"^\s*(dhr|mw|mr|mrs|prof\.?|drs?|dr\.?|ing\.?|ir\.?|jonkheer|baron)\.?\s*", re.I
        )
        self.judge_names = [strip_title.sub("", n).strip() for n in raw]

        # chunked regex list (avoids pattern-size limits)
        chunk, self.judge_regexes = [], []
        for name in self.judge_names:
            chunk.append(re.escape(name))
            if len(chunk) == 400:
                self.judge_regexes.append(re.compile("|".join(chunk), re.I))
                chunk = []
        if chunk:
            self.judge_regexes.append(re.compile("|".join(chunk), re.I))

        # lastname variants for fuzzy layer
        self.judge_variants = {n.split()[-1].lower() for n in self.judge_names}

    # ---------- patterns -----------------------------------------------------
    def _init_patterns(self) -> None:
        initials = r"(?:[A-Z]\.?\s*){1,8}"

        prefixes = [
            "van der", "van den", "van de", "van het", "van", "den", "de", "der", "von",
            "ten", "ter", "te", "'t", "'s", "op de", "op den", "op het", "op",
            "in de", "in den", "in het", "in", "onder de", "onder den", "over de",
            "tot", "uit de", "uit den", "uit het", "uit", "bij de", "bij den",
            "aan de", "aan den", "voor de", "na de", "zonder de"
        ]
        apostro = r"[dDlL]'"
        prefix_re = rf"(?:{ '|'.join(map(re.escape, prefixes)) }|{apostro})"

        surname_core = r"[A-ZÀ-ÖØ-öø-ÿ'’\-][\wÀ-ÖØ-öø-ÿ'’\-]{1,24}"
        self.SURNAME = rf"(?:{prefix_re}\s+|-)?{surname_core}(?:[-\s]{surname_core})*"
        self.INITIALS = initials

        role_words = (
            "gemachtigde", "gedaagde", "eiser", "eiseres", "eisers", "appellant", "appellante",
            "geïntimeerde", "verzoeker", "verzoekers", "verzoekster", "verweerder", "verweerders",
            "verweerster", "betrokkene", "belanghebbende", "rechthebbende", "erfgenaam",
            "advocaat", "raadsman", "raadsvrouw", "procureur", "notaris", "deurwaarder",
            "curator", "bewindvoerder", "voogd", "mentor", "executeur", "liquidateur",
            "syndicus", "rechter", "voorzitter", "raadsheer", "griffier", "officier",
            "directeur", "bestuurder", "commissaris", "aandeelhouder", "schuldenaar",
            "schuldeiser", "crediteur", "debiteur", "huurder", "verhuurder", "koper",
            "verkoper", "werknemer", "werkgever", "patiënt", "behandelaar", "arts"
        )

        rw = '|'.join(role_words)
        title = r"(?:mr\.?|drs?\.?|prof\.?|dr\.?|ing\.?|ir\.?|mevr?\.?)"

        self.ROLE_PATTERNS = [
            re.compile(
                rf"\b(?:{rw})\b\s*[:\-]?\s*(?:de\s+)?(?:heer|dame|heren|dames)?\s*{title}?\s*{initials}?{self.SURNAME}",
                re.I
            ),
            re.compile(
                rf"{self.SURNAME}\s*,?\s*(?:{rw})\b",
                re.I
            ),
            re.compile(
                rf"(?:namens|voor)\s+(?:{rw})\s*[:\-]?\s*(?:de\s+)?(?:heer|dame)?\s*{title}?\s*{initials}?{self.SURNAME}",
                re.I
            )
        ]

        self.FORMAL_PATTERNS = [
            re.compile(rf"(?:de\s+)?(?:heer|dame|heren|dames)\s+{title}?\s*{initials}?{self.SURNAME}", re.I),
            re.compile(rf"{initials}{self.SURNAME}", re.I)
        ]

        self.SIGNATURE_PATTERNS = [
            re.compile(r"^(?:namens\s+(?:deze|de\s+rechtbank|het\s+gerechtshof)|w\.g\.|getekend|ondertekend)\s*[:\-]?.*", re.I)
        ]

        self.BOILERPLATE_PATTERNS = [
            re.compile(
                r"^(?:waarvan opgemaakt dit proces-verbaal|het gerechtshof verklaart het verzet ongegrond|"
                r"aldus vastgesteld|aldus gedaan|aldus gewezen|aldus uitgesproken|"
                r"gewezen door|gegeven door|uitgesproken in het openbaar|"
                r"ten overstaan van|in tegenwoordigheid van|meervoudige kamer|enkelvoudige kamer|"
                r"de rechtbank|het gerechtshof|de hoge raad|het college van beroep)",
                re.I
            )
        ]

        self.ADDRESS_PATTERN = re.compile(
            r"(?:kantoorhoudende|gevestigd|wonende|woonachtig|kantoor\s+houdende)\s+te\s+[^,;\n]{0,100}",
            re.I
        )

    # ---------- internal helpers ---------------------------------------------
    def _judge_sub(self, line: str) -> Tuple[str, int]:
        count = 0
        for rx in self.judge_regexes:
            m = len(rx.findall(line))
            if m:
                line = rx.sub(self.token, line)
                count += m
        return line, count

    def _fuzzy_pass(self, line: str) -> Tuple[str, int]:
        found = 0
        words = re.findall(r'\b[A-ZÀ-ÖØ-öø-ÿ][a-zà-öø-ÿ]{2,}\b', line)
        for w in words:
            lw = w.lower()
            if len(lw) < 4:
                continue
            for v in self.judge_variants:
                if abs(len(lw) - len(v)) > 1:
                    continue
                if SequenceMatcher(None, lw, v).ratio() >= 0.83:
                    line = re.sub(rf"\b{re.escape(w)}\b", self.token, line)
                    found += 1
                    break
        return line, found

    def _over_scrubbed(self, ln: str) -> bool:
        tok = re.escape(self.token)
        if re.search(rf"(?:{tok}\s*){{3,}}", ln):
            return True
        words = ln.split()
        return words.count(self.token) / max(len(words), 1) > 0.6

    # ---------- main public method ------------------------------------------
    def scrub_names(self, text: str, aggressive: bool = True) -> str:
        self.stats = ScrubbingStats()
        out: List[str] = []

        for raw in text.splitlines():
            self.stats.lines_processed += 1
            line = raw.strip()

            if not line:
                out.append("")
                continue

            if any(p.match(line) for p in self.BOILERPLATE_PATTERNS + self.SIGNATURE_PATTERNS):
                self.stats.lines_removed += 1
                continue

            if any(p.fullmatch(line) for p in self.ROLE_PATTERNS):
                self.stats.lines_removed += 1
                continue

            judge_hits = sum(len(rx.findall(line)) for rx in self.judge_regexes)
            if judge_hits >= 2 or any(rx.fullmatch(line) for rx in self.judge_regexes):
                self.stats.lines_removed += 1
                continue

            line, c = self._judge_sub(line)
            self.stats.names_found += c

            for pat in self.ROLE_PATTERNS + self.FORMAL_PATTERNS:
                hits = len(pat.findall(line))
                if hits:
                    line = pat.sub(self.token, line)
                    self.stats.names_found += hits

            line = self.ADDRESS_PATTERN.sub(
                lambda m: re.sub(self.SURNAME, self.token, m.group()), line
            )

            if aggressive:
                line, c = self._fuzzy_pass(line)
                self.stats.names_found += c

            tok = re.escape(self.token)
            line = re.sub(rf"(?:{tok}\s*)+", self.token + " ", line)
            line = re.sub(r"\s+", " ", line).strip(" ,.–\u00A0")

            if self._over_scrubbed(line) or len(line) <= 3 or line == self.token:
                self.stats.lines_removed += 1
                continue

            out.append(line)

        cleaned, blank = [], False
        for ln in out:
            if ln:
                cleaned.append(ln)
                blank = False
            elif not blank:
                cleaned.append("")
                blank = True

        return "\n".join(cleaned).strip()

    # ---------- simple stats getter -----------------------------------------
    def get_scrubbing_report(self) -> Dict:
        return {
            "lines_processed": self.stats.lines_processed,
            "lines_removed":   self.stats.lines_removed,
            "names_found":     self.stats.names_found,
            "removal_rate":    self.stats.lines_removed / max(self.stats.lines_processed, 1)
        }

# ---------------------------------------------------------------------------
#  Factory + thin wrapper (matches old API)
# ---------------------------------------------------------------------------
def scrub_names(text: str) -> str:
    judge_path = Path(__file__).with_name("judge_names.json")
    scrubber = UltraNameScrubber(judge_path)
    cleaned = scrubber.scrub_names(text, aggressive=True)
    rep = scrubber.get_scrubbing_report()
    print(f"[DEBUG] scrubbed {rep['names_found']} names, removed {rep['lines_removed']} lines")
    return cleaned

# ---------------------------------------------------------------------------
#  Scraping helpers
# ---------------------------------------------------------------------------
HF_REPO     = "vGassen/dutch-court-cases-rechtspraak"
API_URL     = "https://data.rechtspraak.nl/uitspraken/zoeken"
CONTENT_URL = "https://data.rechtspraak.nl/uitspraken/content"

def fetch_eclis(max_items: int = 250) -> List[str]:
    print("[INFO] fetching ECLI list …")
    params = {"type": "uitspraak", "return": "DOC", "max": max_items}
    r = requests.get(API_URL, params=params)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    return [
        e.find("{http://www.w3.org/2005/Atom}id").text
        for e in root.findall("{http://www.w3.org/2005/Atom}entry")
    ]

def fetch_uitspraak(ecli: str) -> str:
    try:
        r = requests.get(f"{CONTENT_URL}?id={ecli}")
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {"rs": "http://www.rechtspraak.nl/schema/rechtspraak-1.0"}
        el = root.find(".//rs:uitspraak", ns)
        if el is not None:
            return ET.tostring(el, encoding="unicode", method="text").strip()
    except Exception as exc:
        print(f"[WARN] could not fetch {ecli}: {exc}")
    return ""

def save_to_jsonl(rows: List[Dict], out_path: Path) -> None:
    with out_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

# ---------------------------------------------------------------------------
#  Main routine
# ---------------------------------------------------------------------------
def main() -> None:
    try:
        from datasets import Dataset
        from huggingface_hub import login
    except ImportError as e:
        print("[ERROR] install datasets & huggingface_hub first:", e)
        sys.exit(1)

    token = os.getenv("HF_TOKEN")
    if not token:
        raise ValueError("HF_TOKEN environment variable not set")
    login(token=token)
    print("[INFO] logged in to HuggingFace Hub")

    eclis = fetch_eclis()
    jot = Path("uitspraken.jsonl")
    judge_file = Path(__file__).with_name("judge_names.json")
    scrubber = UltraNameScrubber(judge_file)

    records = []
    for ecli in eclis:
        raw = fetch_uitspraak(ecli)
        if not raw:
            continue
        cleaned = scrubber.scrub_names(raw, aggressive=True)
        records.append({"ecli": ecli, "uitspraak": cleaned})

    print(f"[INFO] collected {len(records)} uitspraken")
    if not records:
        print("[WARN] nothing to upload")
        return

    save_to_jsonl(records, jot)
    ds = Dataset.from_json(str(jot))
    ds.push_to_hub(HF_REPO)
    print("[INFO] upload complete")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
