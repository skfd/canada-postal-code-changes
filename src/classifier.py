"""Classify city_changed events into subtypes (encoding, boundary, rename, etc.).

Loads rules from data/city_change_rules.json and applies them to determine
whether a city name change is technical (encoding, accent, punctuation) or
substantive (boundary reassignment, official rename, etc.).
"""

import json
import logging
import unicodedata
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

RULES_PATH = Path("data/city_change_rules.json")

# Mojibake marker characters that appear when UTF-8 accented chars are
# decoded through a wrong single-byte encoding (e.g. latin-1 -> UTF-8 round-trip).
_MOJIBAKE_CHARS = set("ãÃâÂêÊîÎôÔûÛëËïÏüÜçÇ")


@lru_cache(maxsize=1)
def _load_rules() -> dict:
    """Load and cache the rules file."""
    if not RULES_PATH.exists():
        logger.warning("Rules file not found at %s, using empty rules", RULES_PATH)
        return {}
    with open(RULES_PATH, encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=1)
def _boundary_lookup() -> set[frozenset[str]]:
    """Build a set of frozensets for O(1) boundary pair lookup.

    City names are stored lowercased and accent-stripped for fuzzy matching,
    since the data may have mojibake or stripped accents.
    """
    rules = _load_rules()
    pairs = set()
    for entry in rules.get("known_boundary_pairs", []):
        cities = entry["cities"]
        pair = frozenset(_normalize(c) for c in cities)
        pairs.add(pair)
    return pairs


@lru_cache(maxsize=1)
def _rename_lookup() -> dict[tuple[str, str], str]:
    """Build old->new rename lookup (normalized). Returns {(old,new): note}."""
    rules = _load_rules()
    result = {}
    for entry in rules.get("known_renames", []):
        key = (_normalize(entry["old"]), _normalize(entry["new"]))
        result[key] = entry.get("note", "")
    return result


@lru_cache(maxsize=1)
def _abbreviation_pairs() -> list[tuple[str, str]]:
    """Load abbreviation expansion pairs."""
    rules = _load_rules()
    return [
        (entry["short"].lower(), entry["long"].lower())
        for entry in rules.get("abbreviation_patterns", [])
    ]


def _normalize(name: str) -> str:
    """Lowercase, strip accents, and collapse whitespace/hyphens."""
    name = name.lower().strip()
    # Strip unicode accents
    name = "".join(
        c for c in unicodedata.normalize("NFD", name)
        if unicodedata.category(c) != "Mn"
    )
    # Remove mojibake characters too
    name = name.replace("ã", "").replace("â", "")
    # Normalize separators
    name = name.replace("-", " ").replace(".", "").replace("'", "")
    # Collapse whitespace
    return " ".join(name.split())


def _strip_accents(name: str) -> str:
    """Strip accents but keep case and structure."""
    return "".join(
        c for c in unicodedata.normalize("NFD", name)
        if unicodedata.category(c) != "Mn"
    )


def _has_mojibake(text: str) -> bool:
    """Check if text contains mojibake from encoding corruption.

    Patterns detected:
    - C0/C1 control characters (U+0080-U+009F) — never appear in real city names
    - ã/Ã adjacent to control characters (common UTF-8 misinterpretation)
    - ã/Ã followed by an uppercase letter mid-word (e.g. MontrãAl)
    """
    for i, ch in enumerate(text):
        # C0/C1 control characters are a dead giveaway of encoding issues
        if 0x0080 <= ord(ch) <= 0x009F:
            return True
        # ã/Ã followed by uppercase letter mid-word
        if ch in _MOJIBAKE_CHARS and i + 1 < len(text):
            next_ch = text[i + 1]
            if next_ch.isupper():
                return True
    return False


def classify_city_change(old_value: str | None, new_value: str | None) -> str:
    """Classify a single city_changed event into a subtype.

    Args:
        old_value: Previous city name.
        new_value: New city name.

    Returns:
        One of: encoding, accent_normalization, punctuation, spacing,
        abbreviation, boundary, rename, substantive.
    """
    if not old_value or not new_value:
        return "substantive"

    old = old_value.strip()
    new = new_value.strip()

    if old == new:
        return "encoding"  # shouldn't happen, but just in case

    # 1. Encoding/mojibake: either side has mojibake characters
    if _has_mojibake(old) or _has_mojibake(new):
        return "encoding"

    # 2. Accent normalization: same string after stripping accents
    if _strip_accents(old).lower() == _strip_accents(new).lower() and old.lower() != new.lower():
        return "accent_normalization"

    # 3. Punctuation: trailing period removal or addition
    if old.rstrip(".") == new.rstrip("."):
        return "punctuation"

    # 4. Spacing: hyphen vs space, same letters otherwise
    old_collapsed = old.lower().replace("-", " ").replace("  ", " ").strip()
    new_collapsed = new.lower().replace("-", " ").replace("  ", " ").strip()
    if old_collapsed == new_collapsed:
        return "spacing"

    # 5. Abbreviation: St/Saint, Ste/Sainte, Mt/Mount
    for short, long in _abbreviation_pairs():
        old_low = old.lower()
        new_low = new.lower()
        # Check if one uses the short form and the other uses the long form
        if (old_low.startswith(short) and new_low.startswith(long)
                and old_low.replace(short, long, 1) == new_low):
            return "abbreviation"
        if (old_low.startswith(long) and new_low.startswith(short)
                and old_low.replace(long, short, 1) == new_low):
            return "abbreviation"

    # 6. Known renames
    old_norm = _normalize(old)
    new_norm = _normalize(new)
    if (old_norm, new_norm) in _rename_lookup():
        return "rename"

    # 7. Known boundary pairs
    pair = frozenset([old_norm, new_norm])
    if pair in _boundary_lookup():
        return "boundary"

    # 8. Default
    return "substantive"


def classify_batch(rows: list[tuple[str | None, str | None]]) -> list[str]:
    """Classify a batch of (old_value, new_value) pairs.

    Returns a list of subtype strings in the same order.
    """
    return [classify_city_change(old, new) for old, new in rows]
