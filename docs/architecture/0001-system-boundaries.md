# ADR 0001: System boundaries and source of truth

## Status

Accepted for Phase 1.

## Context

SCEI must reconstruct the physical history of pharmaceutical materials from heterogeneous and incomplete records. Similar text is not sufficient evidence of physical identity, and an LLM or vector index must not become the authoritative record.

## Decision

SCEI is organized into these bounded layers:

1. **Ingestion** - accepts ERP/WMS/MES exports, documents, APIs, and future connectors.
2. **Document intelligence** - parses files, preserves layout, classifies documents, and extracts claims with provenance.
3. **Identity and validation** - resolves organizations, facilities, materials, batches, and products while preserving ambiguity.
4. **Canonical event platform** - stores immutable source records and validated event versions.
5. **Evidence projections** - projects canonical events into PostgreSQL, Neo4j, lexical search, and vector search.
6. **Investigation engine** - performs deterministic temporal and multi-hop genealogy calculations.
7. **Intelligence API** - exposes evidence-backed findings, uncertainty, and reports.
8. **Human review** - approves, rejects, or escalates ambiguous relationships and high-impact findings.

The canonical event store and its provenance are the source of truth. Neo4j, Qdrant, OpenSearch, and Ollama are derived or intelligence layers.

## Consequences

- Ingestion is asynchronous and idempotent.
- Every derived claim must reference one or more source records.
- Reprocessing can reproduce projections from immutable inputs.
- The system can say that a relationship is unknown instead of fabricating certainty.
- More infrastructure is required than a basic vector-RAG prototype, but recall investigations become auditable.
