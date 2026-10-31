# ADR 0003: Provenance-preserving document intelligence

## Decision

Document extraction produces layout-aware segments and claims. Every claim retains its source locator, extraction method, and confidence. Original documents remain outside the derived text representation and are referenced by document ID.

The deterministic extraction layer handles text, CSV, Excel, DOCX, and embedded PDF text. A PDF with no embedded text is marked `requires_ocr` rather than silently treated as empty. OCR and Ollama-based semantic extraction will be additional stages that consume these segments and preserve the same provenance contract.

## Consequences

- Table relationships are retained through row and column coordinates.
- Scanned documents are detectable and can be routed to an OCR worker.
- Regex claims are useful for high-precision identifiers and dates but are not treated as complete document understanding.
- Model-generated claims must use the same claim and locator schema.
