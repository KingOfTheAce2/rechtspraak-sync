import json
import re
from pathlib import Path

# Load judge names once
_JUDGE_NAMES_FILE = Path(__file__).parent / "judge_names.json"
try:
    with open(_JUDGE_NAMES_FILE, "r", encoding="utf-8") as f:
        _JUDGE_NAMES = json.load(f)
except Exception:
    _JUDGE_NAMES = []

# Pre-compile regex patterns for faster replacement
_JUDGE_PATTERNS = [re.compile(re.escape(name), re.IGNORECASE) for name in _JUDGE_NAMES]

# Simple gemachtigde pattern: match a few tokens following the keyword
_GEMACHTIGDE_PATTERN = re.compile(
    r"(?i)(gemachtigde[^\n]{0,10}(?:mr\.\s*)?)((?:[A-Za-zÀ-ÖØ-öø-ÿ.'`-]+\s*){1,5})"
)

def scrub_judge_names(text: str) -> str:
    """Replace known judge names with the placeholder 'NAAM'."""
    if not text:
        return text
    for pattern in _JUDGE_PATTERNS:
        text = pattern.sub("NAAM", text)
    return text

def scrub_gemachtigde_names(text: str) -> str:
    """Replace names following 'gemachtigde' with 'NAAM'."""
    if not text:
        return text
    return _GEMACHTIGDE_PATTERN.sub(lambda m: f"{m.group(1)}NAAM", text)
