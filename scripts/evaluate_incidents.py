"""Evaluate projected genealogy against synthetic incident ground truth."""

from __future__ import annotations

import argparse
import gzip
import json
import tempfile
from pathlib import Path
from uuid import UUID

from scei.graph.projector import GenealogyProjector
from scei.ingestion.models import EventIngestEnvelope
from scei.investigation.genealogy import GenealogyEngine
from scei.resolution.models import EntityProfile
from scei.resolution.resolver import EntityResolver
from scei.storage.sqlite import SQLiteCanonicalStore


def rows(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            yield json.loads(line)


def evaluate(dataset: Path, max_events: int | None = None) -> dict[str, float | int]:
    organizations = [
        UUID(row["organization_id"]) for row in rows(dataset / "organizations.jsonl.gz")
    ]
    batch_rows = list(rows(dataset / "batches.jsonl.gz"))
    product_rows = list(rows(dataset / "products.jsonl.gz"))
    batch_ids = {row["batch_code"]: UUID(row["batch_id"]) for row in batch_rows}
    profiles = [
        EntityProfile(
            entity_id=UUID(row["batch_id"]),
            organization_id=organizations[row["organization_index"]],
            entity_type="batch",
            canonical_name=row["batch_code"],
            identifiers=[row["batch_code"], row["legacy_batch_code"]],
        )
        for index, row in enumerate(batch_rows)
    ]
    profiles.extend(
        EntityProfile(
            entity_id=UUID(row["product_id"]),
            organization_id=organizations[index % len(organizations)],
            entity_type="product",
            canonical_name=row["brand_name"],
            identifiers=[row["product_code"]],
        )
        for index, row in enumerate(product_rows)
    )
    resolver = EntityResolver(profiles)
    with tempfile.TemporaryDirectory() as temp_dir:
        store = SQLiteCanonicalStore(Path(temp_dir) / "evaluation.db")
        projector = GenealogyProjector(store)
        projected_events = 0
        for index, row in enumerate(rows(dataset / "events.jsonl.gz")):
            if max_events is not None and index >= max_events:
                break
            organization_id = organizations[row["organization_index"]]
            event = EventIngestEnvelope(
                organization_id=organization_id,
                event_id=UUID(row["event_id"]),
                event_type=row["event_type"],
                event_time=row["event_time"],
                idempotency_key=f"eval:{row['event_id']}",
                source_system=row["source_system"],
                payload={
                    key: value
                    for key, value in row.items()
                    if key
                    not in {
                        "event_id",
                        "organization_index",
                        "event_type",
                        "event_time",
                        "source_system",
                        "provenance",
                    }
                },
                provenance=[row["provenance"]],
            )
            bindings = {}
            for key in [
                "batch_code",
                "source_batch_code",
                "destination_batch_code",
                "input_batch_code",
                "product_code",
            ]:
                value = event.payload.get(key)
                if value:
                    entity_type = "product" if key == "product_code" else "batch"
                    bindings[str(value)] = resolver.resolve(
                        organization_id, entity_type, str(value)
                    )
            result = projector.project_event(event, bindings)
            projected_events += int(result.edges_written > 0 or result.nodes_written > 0)
        incidents = list(rows(dataset / "incidents.jsonl.gz"))
        evaluated, expected_total, found_total = 0, 0, 0
        for incident in incidents:
            contaminated_id = batch_ids.get(incident["contaminated_batch_code"])
            if contaminated_id is None:
                continue
            organization_id = organizations[
                next(row for row in batch_rows if row["batch_id"] == str(contaminated_id))[
                    "organization_index"
                ]
            ]
            traversal = GenealogyEngine(store).traverse(
                organization_id, contaminated_id, direction="downstream"
            )
            found = {str(node_id) for node_id in traversal.visited_node_ids}
            expected = {
                str(batch_ids[code])
                for code in incident["expected_affected_batch_codes"]
                if code in batch_ids
            }
            evaluated += 1
            expected_total += len(expected)
            found_total += len(expected & found)
        store.close()
    return {
        "projected_events": projected_events,
        "incidents_evaluated": evaluated,
        "expected_affected_batches": expected_total,
        "matched_affected_batches": found_total,
        "affected_batch_recall": round(found_total / expected_total, 4) if expected_total else 1.0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/generated/bangladesh_pharma_smoke")
    )
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args()
    print(json.dumps(evaluate(args.dataset, args.max_events), indent=2))
