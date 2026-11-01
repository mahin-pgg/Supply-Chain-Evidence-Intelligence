"""Project canonical events from a generated reference dataset into SQLite graph tables."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from uuid import UUID

from scei.graph.projection_service import GraphProjectionService
from scei.resolution.models import EntityProfile
from scei.storage.sqlite import SQLiteCanonicalStore


def rows(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            yield json.loads(line)


def load_profiles(dataset: Path) -> list[EntityProfile]:
    organizations = [UUID(row["organization_id"]) for row in rows(dataset / "organizations.jsonl.gz")]
    profiles = []
    batch_rows = list(rows(dataset / "batches.jsonl.gz"))
    for row in batch_rows:
        profiles.append(
            EntityProfile(
                entity_id=UUID(row["batch_id"]),
                organization_id=organizations[row["organization_index"]],
                entity_type="batch",
                canonical_name=row["batch_code"],
                identifiers=[row["batch_code"], row["legacy_batch_code"]],
            )
        )
    for index, row in enumerate(rows(dataset / "products.jsonl.gz")):
        profiles.append(
            EntityProfile(
                entity_id=UUID(row["product_id"]),
                organization_id=organizations[index % len(organizations)],
                entity_type="product",
                canonical_name=row["brand_name"],
                identifiers=[row["product_code"]],
            )
        )
    return profiles


def project(dataset: Path, database: Path, max_events: int | None = None) -> dict[str, object]:
    store = SQLiteCanonicalStore(database)
    events = store.list_event_envelopes(limit=max_events)
    summary = GraphProjectionService(store, load_profiles(dataset)).project(events)
    store.close()
    return {
        "events_seen": summary.events_seen,
        "events_projected": summary.events_projected,
        "nodes_written": summary.nodes_written,
        "edges_written": summary.edges_written,
        "gaps": summary.gaps,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=Path("data/generated/bangladesh_pharma_large"))
    parser.add_argument("--database", type=Path, default=Path("data/local/scei.db"))
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(project(args.dataset, args.database, args.max_events), indent=2))
