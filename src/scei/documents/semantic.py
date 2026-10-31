from __future__ import annotations

from scei.documents.models import ExtractedClaim, ExtractedDocument
from scei.llm.ollama import OllamaClient, OllamaUnavailable
from scei.resolution.normalization import normalize_identifier


class SemanticDocumentEnricher:
    """Optional Ollama enrichment that never replaces deterministic evidence."""

    def __init__(self, client: OllamaClient | None = None) -> None:
        self.client = client or OllamaClient()

    def classify_and_extract(self, document: ExtractedDocument) -> dict[str, object]:
        context = "\n".join(segment.text for segment in document.segments[:80])
        schema = {
            "type": "object",
            "properties": {
                "document_type": {"type": "string"},
                "claims": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field_name": {"type": "string"},
                            "value": {"type": "string"},
                            "confidence": {"type": "number"},
                        },
                        "required": ["field_name", "value", "confidence"],
                    },
                },
            },
            "required": ["document_type", "claims"],
        }
        prompt = (
            "Extract supply-chain document type and claims. Do not invent values; return only claims supported by the supplied text.\n\n"
            + context
        )
        try:
            return self.client.generate_json(prompt, schema)
        except OllamaUnavailable as error:
            return {
                "document_type": document.kind.value,
                "claims": [],
                "fallback": "deterministic_only",
                "warning": str(error),
            }

    def enrich(self, document: ExtractedDocument) -> ExtractedDocument:
        """Add only model claims that can be located in the source text.

        The model is an intelligence layer, not an authority. Claims that cannot
        be anchored to an extracted segment are discarded instead of becoming
        unsupported evidence.
        """
        result = self.classify_and_extract(document)
        existing = {
            (claim.field_name, claim.normalized_value or claim.value.casefold(), claim.locator.model_dump_json())
            for claim in document.claims
        }
        claims = list(document.claims)
        for item in result.get("claims", []):
            if not isinstance(item, dict):
                continue
            field_name = str(item.get("field_name", "")).strip()
            value = str(item.get("value", "")).strip()
            if not field_name or not value:
                continue
            segment = next(
                (candidate for candidate in document.segments if value.casefold() in candidate.text.casefold()),
                None,
            )
            if segment is None:
                continue
            try:
                confidence = max(0.0, min(1.0, float(item.get("confidence", 0.0))))
            except (TypeError, ValueError):
                continue
            normalized = normalize_identifier(value) if "batch" in field_name.casefold() else value
            key = (field_name, normalized, segment.locator.model_dump_json())
            if key in existing:
                continue
            claims.append(
                ExtractedClaim(
                    field_name=field_name,
                    value=value,
                    normalized_value=normalized,
                    locator=segment.locator,
                    extraction_method="ollama:semantic",
                    confidence=confidence,
                )
            )
            existing.add(key)
        return document.model_copy(update={"claims": claims})
