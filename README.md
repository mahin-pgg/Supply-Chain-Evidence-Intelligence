# Supply Chain Evidence Intelligence

SCEI is an evidence-backed investigation platform for reconstructing pharmaceutical material genealogy across suppliers, facilities, batches, shipments, inventory, and finished products.

The system treats validated supply-chain events as the source of truth. Graph retrieval, search, and local language models are intelligence layers over that evidence; they are not authoritative data stores.

## Project status

Phase 1 - repository bootstrap and architecture contracts.

## Initial product boundary

- Pharmaceutical batch-contamination investigations.
- Multi-tenant organization isolation.
- Synthetic but realistic, messy development data.
- Local Docker deployment first.
- Ollama-based local inference.
- Human review for ambiguous or high-impact findings.

## Development principles

1. Every finding must link back to source evidence.
2. A graph edge is a verified or explicitly uncertain assertion, never an LLM guess.
3. Raw source documents are immutable; derived data is versioned and reproducible.
4. Deterministic graph computation precedes agentic explanation.
5. Uncertainty and missing evidence are first-class outputs.

## Planned stack

Python, FastAPI, PostgreSQL, Neo4j, Qdrant, OpenSearch, Kafka/Redpanda, MinIO/S3, Temporal, LangGraph, Ollama, Docker Compose, and OpenTelemetry.

See [architecture decision 0001](docs/architecture/0001-system-boundaries.md) and the [domain model](docs/architecture/domain-model.md).
