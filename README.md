# Supply Chain Evidence Intelligence

SCEI is an evidence-backed investigation platform for reconstructing pharmaceutical material genealogy across suppliers, facilities, batches, shipments, inventory, and finished products.

The system treats validated supply-chain events as the source of truth. Graph retrieval, search, and local language models are intelligence layers over that evidence; they are not authoritative data stores.

## Project status

The core local investigation workflow is implemented through registry loading, dataset versioning, resolution review, graph projection, genealogy traversal, contamination reporting, evaluation, and optional Ollama assistance. The current deployment target is local Docker with SQLite persistence. Production hardening, authentication, richer evidence ingestion, and external storage adapters remain on the roadmap.

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

## End-to-end project flow

~~~text
Generate messy Bangladesh data
        |
        v
Register dataset/version/load run
        |
        v
Load canonical registry and source events
        |
        v
Resolve identifiers and create review items for gaps
        |
        v
Approve or reject resolutions with immutable audit history
        |
        v
Apply approved mappings and reproject genealogy idempotently
        |
        v
Investigate upstream/downstream contamination paths
        |
        v
Review evidence, provenance, uncertainty, and impact
~~~

The canonical registry and source events are authoritative. Graph nodes and edges are versioned projections; they are not the identity authority. Ollama can explain grounded evidence, but it cannot approve resolutions, invent relationships, or bypass tenant and provenance rules.

## Implemented capabilities

- Multi-tenant isolation across organizations, workspaces, registry records, reviews, datasets, and graph projections.
- Dataset registry, versions, manifests, checksums, load runs, activation state, and investigation contexts.
- Canonical registry for batches, products, suppliers, facilities, and materials.
- Typed identifiers, aliases, validity windows, lifecycle state, provenance, supersession, and identity conflicts.
- Deterministic registry-backed projection for RECEIVE, TRANSFORM, and PRODUCE events.
- Facility-aware genealogy edges such as RECEIVED_AT, TRANSFORMED_AT, and PRODUCED_AT.
- Idempotent graph reprojection with immutable projection metadata.
- Resolution review lifecycle with append-only decisions and retry-safe application.
- Configurable upstream/downstream traversal with hop and path limits.
- Contamination incident reports with expected-vs-observed impact and completeness warnings.
- Local investigation console at /ui and review queue at /ui/reviews.
- Deterministic evaluation metrics at /v1/evaluation/summary.
- Optional grounded Ollama review assistance using qwen3:4b.

## Recommended local workflow

### 1. Create the environment

~~~powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -e ".[documents]"
~~~

### 2. Generate the development dataset

The generator creates deterministic, gzip-compressed JSONL with realistic Bangladesh pharmaceutical supply-chain messiness, identifier corruption, aliases, manufacturing events, and contamination incidents. Generated data is ignored by Git.

~~~powershell
python scripts/generate_bangladesh_dataset.py
~~~

See [the data workflow](docs/data/README.md) for generator options, seeds, manifests, and dataset layout.

### 3. Load a fresh smoke database

Do not modify or migrate the historical large database at data/local/scei.db. Use an isolated database for validation:

~~~powershell
python scripts/load_registry_smoke.py --dataset data/generated/bangladesh_pharma_large --database data/local/scei-registry-smoke.db
python scripts/load_genealogy_smoke.py --dataset data/generated/bangladesh_pharma_large --database data/local/scei-genealogy-smoke.db
~~~

The genealogy loader registers batches, products, suppliers, facilities, and materials, loads canonical events, and creates the dataset context.

### 4. Project and evaluate

~~~powershell
python scripts/project_graph.py --dataset data/generated/bangladesh_pharma_large --database data/local/scei-genealogy-smoke.db
python scripts/evaluate_smoke.py --database data/local/scei-genealogy-smoke.db
~~~

Projection is tenant-scoped, registry-backed, provenance-preserving, and safe to replay. Unresolved identifiers become review items instead of guessed graph relationships.

### 5. Start the API and use the UI

~~~powershell
\.venv\Scripts\python.exe -m uvicorn scei.api.app:app --reload
~~~

- API documentation: http://127.0.0.1:8000/docs
- Investigation console: http://127.0.0.1:8000/ui
- Resolution review queue: http://127.0.0.1:8000/ui/reviews
- Health check: http://127.0.0.1:8000/health

Use POST /v1/investigations/batch for a batch investigation and POST /v1/genealogy/traverse for direct upstream/downstream traversal. Returned paths include relationship state, source-event provenance, document references, quantity information, tenant context, projection identifiers, and completeness warnings.

### 6. Resolve gaps and reproject

Use GET /v1/reviews/resolution to list tenant-scoped pending reviews. Approve or reject a review through the review API. Approved mappings are applied through the existing mapping and projection services; decisions remain append-only and replay-safe. A rejected or pending review cannot create a graph relationship.

## Ollama assistance

Ollama assistance is advisory only. It receives grounded review evidence and returns a recommendation with citations; it cannot approve or reject a review or create graph data.

~~~powershell
$env:SCEI_ENABLE_SEMANTIC = "1"
$env:OLLAMA_MODEL = "qwen3:4b"
$env:OLLAMA_BASE_URL = "http://127.0.0.1:11434"
\.venv\Scripts\python.exe -m uvicorn scei.api.app:app --reload
~~~

If Ollama is unavailable, the review-assistance endpoint returns a deterministic advisory fallback.

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

Projection gaps are persisted in the resolution review queue. Use `GET /v1/reviews/resolution` to list tenant-scoped pending reviews and `POST /v1/reviews/resolution/{review_id}` to approve or reject a proposed resolution.

## Genealogy investigation

Use `POST /v1/genealogy/traverse` with `direction`, `max_hops`, and `include_possible`. Each returned path includes the relationship state, source event, document provenance, quantity flow, and completeness warnings. By default, the API traverses only verified and probable relationships; set `include_possible` to `true` when exploratory possible links are required.

## Synthetic development data

The Bangladesh-oriented data generator is documented in [docs/data/README.md](docs/data/README.md). It produces deterministic, gzip-compressed JSONL with realistic supply-chain messiness and ground-truth incident impact. Generated data is intentionally ignored by Git.

## Local Docker deployment

Copy `.env.docker.example` to `.env.docker`, then start the API:

```powershell
Copy-Item .env.docker.example .env.docker
docker compose up --build
```

The API is available at `http://127.0.0.1:8000/docs`; the investigation console is at `http://127.0.0.1:8000/ui`. SQLite persists in `data/local/scei.db`. Ollama remains on the host through `host.docker.internal`; set `OLLAMA_MODEL` in `.env.docker` to the installed local model.

Create a recoverable database backup with:

```powershell
.\scripts\docker_backup.ps1
```
