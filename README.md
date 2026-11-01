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

## Local semantic document enrichment

Deterministic extraction is always performed first. To enable optional Ollama enrichment for uploaded documents, set the feature flag before starting the API:

```powershell
$env:SCEI_ENABLE_SEMANTIC = "1"
\.venv\Scripts\python.exe -m uvicorn scei.api.app:app --reload
```

The local default model is `qwen3:4b`. The semantic layer only persists claims that can be anchored to extracted source text; unsupported model output is discarded. Override the model with `OLLAMA_MODEL` when needed.

## Batch investigation API

Use `POST /v1/investigations/batch` with an organization ID and a batch identifier. The response combines normalized evidence citations with genealogy and contamination impact when the batch has been projected into the graph. Missing graph projections are reported in `gaps` rather than hidden.

To replay canonical events into the local graph after loading the generated dataset:

```powershell
\.venv\Scripts\python.exe scripts\project_graph.py `
  --dataset data/generated/bangladesh_pharma_large `
  --database data/local/scei.db
```

Projection uses exact tenant-scoped identifiers by default. Unresolved or corrupted identifiers are reported as gaps for review instead of being guessed into graph relationships.

## Synthetic development data

The Bangladesh-oriented data generator is documented in [docs/data/README.md](docs/data/README.md). It produces deterministic, gzip-compressed JSONL with realistic supply-chain messiness and ground-truth incident impact. Generated data is intentionally ignored by Git.
