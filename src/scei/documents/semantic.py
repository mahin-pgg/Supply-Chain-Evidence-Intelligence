from __future__ import annotations

from scei.documents.models import ExtractedDocument
from scei.llm.ollama import OllamaClient, OllamaUnavailable


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
