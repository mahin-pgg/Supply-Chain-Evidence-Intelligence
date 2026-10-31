from __future__ import annotations

import re
import unicodedata


def normalize_text(value: str) -> str:
    """Normalize text without pretending that normalization proves identity."""
    value = unicodedata.normalize("NFKC", value).casefold().strip()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_identifier(value: str) -> str:
    """Create a comparison key while retaining the original value elsewhere."""
    return re.sub(r"[^a-z0-9]+", "", normalize_text(value))


def normalize_name(value: str) -> str:
    value = normalize_text(value)
    value = re.sub(r"\b(limited|ltd|company|co|inc|corporation|corp)\b", " ", value)
    return re.sub(r"[^\w\u0980-\u09ff]+", " ", value).strip()


def name_tokens(value: str) -> frozenset[str]:
    return frozenset(token for token in normalize_name(value).split() if len(token) > 1)


def identifier_variants(value: str) -> frozenset[str]:
    """Return conservative keys; OCR correction remains a resolver signal, not a fact."""
    normalized = normalize_identifier(value)
    variants = {normalized}
    if "0" in normalized:
        variants.add(normalized.replace("0", "o"))
    if "1" in normalized:
        variants.add(normalized.replace("1", "i"))
    return frozenset(variant for variant in variants if variant)
