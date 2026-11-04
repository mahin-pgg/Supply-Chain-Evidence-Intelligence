"""Load products/events and project registry-backed genealogy in a fresh DB."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid5

from load_registry_smoke import load, rows

from scei.datasets.sqlite import SQLiteDatasetRepository
from scei.graph.registry_projection import RegistryGraphProjectionService
from scei.ingestion.models import EventIngestEnvelope
from scei.registry.models import (
    CanonicalEntity,
    EntityDatasetMembership,
    EntityIdentifier,
    RegistryEntityType,
    RegistryProvenance,
)
from scei.registry.sqlite import SQLiteRegistryRepository
from scei.storage.sqlite import SQLiteCanonicalStore


def enrich_and_project(dataset: Path, database: Path) -> dict[str, object]:
    base = load(dataset, database)
    version_id = UUID(str(base["dataset_version_id"]))
    organizations = list(rows(dataset / "organizations.jsonl.gz"))
    facilities = list(rows(dataset / "facilities.jsonl.gz"))
    facility_codes = {index: facility[1]["facility_code"] for index, facility in enumerate(facilities)}
    batch_organizations = {
        batch["batch_code"]: UUID(organizations[batch["organization_index"]][1]["organization_id"])
        for _, batch in rows(dataset / "batches.jsonl.gz")
    }
    datasets = SQLiteDatasetRepository(database)
    registry = SQLiteRegistryRepository(database)
    graph = SQLiteCanonicalStore(database)
    entity_counts: dict[str, int] = {}
    entity_specs = [
        ("products.jsonl.gz", "product_id", "product_code", "brand_name", "product", RegistryEntityType.PRODUCT),
        ("suppliers.jsonl.gz", "supplier_id", "supplier_code", "legal_name", "supplier", RegistryEntityType.SUPPLIER),
        ("facilities.jsonl.gz", "facility_id", "facility_code", "name", "facility", RegistryEntityType.FACILITY),
        ("materials.jsonl.gz", "material_id", "material_code", "name", "material", RegistryEntityType.MATERIAL),
    ]
    for filename, id_key, code_key, name_key, entity_label, entity_type in entity_specs:
        count = 0
        for _, entity in rows(dataset / filename):
            for _, organization in organizations:
                organization_id = UUID(organization["organization_id"])
                entity_id = UUID(entity[id_key])
                provenance_id = uuid5(entity_id, f"{entity_label}:{version_id}:{organization_id}")
                registry.add_entity(CanonicalEntity(
                    entity_id=entity_id, organization_id=organization_id,
                    entity_type=entity_type,
                    canonical_name=entity[name_key], identity_origin=f"dataset:{version_id}",
                    attributes={k: str(v) for k, v in entity.items() if k not in {id_key, "synthetic"}},
                ))
                registry.add_provenance(RegistryProvenance(
                    provenance_id=provenance_id, organization_id=organization_id, entity_id=entity_id,
                    dataset_version_id=version_id, source_file=filename,
                    source_row_reference=str(entity[id_key]),
                ))
                registry.add_identifier(EntityIdentifier(
                    identifier_id=uuid5(entity_id, f"{entity_label}-code:{organization_id}"),
                    organization_id=organization_id, entity_id=entity_id,
                    identifier_type=f"{entity_label}_code", namespace="synthetic_generator",
                    raw_value=entity[code_key], normalized_value=entity[code_key].lower(),
                    is_primary=True,
                ))
                registry.add_membership(EntityDatasetMembership(
                    membership_id=uuid5(entity_id, f"{entity_label}-membership:{version_id}:{organization_id}"),
                    organization_id=organization_id, entity_id=entity_id,
                    dataset_version_id=version_id, source_entity_key=entity[id_key],
                    source_table=entity_label + "s", provenance_id=provenance_id,
                ))
                count += 1
        entity_counts[entity_label] = count
    store_events = 0
    for row_number, event in rows(dataset / "events.jsonl.gz"):
        organization_id = UUID(organizations[event["organization_index"]][1]["organization_id"])
        payload = dict(event)
        payload.pop("organization_index", None)
        payload.pop("event_id", None)
        payload.pop("event_type", None)
        payload.pop("event_time", None)
        payload.pop("source_system", None)
        payload.pop("synthetic", None)
        if "facility_index" in payload:
            payload["facility_code"] = facility_codes[payload.pop("facility_index")]
        store_events += graph.add_event(EventIngestEnvelope(
            organization_id=organization_id, event_id=UUID(event["event_id"]),
            event_type=event["event_type"], event_time=datetime.fromisoformat(event["event_time"]),
            idempotency_key=event["event_id"], source_system=event["source_system"],
            payload=payload, provenance=[event["provenance"]],
        ))
    incident_count = 0
    for _, incident in rows(dataset / "incidents.jsonl.gz"):
        organization_id = batch_organizations[incident["contaminated_batch_code"]]
        incident_count += graph.add_contamination_incident(
            organization_id, incident["incident_id"], incident["severity"],
            incident["contaminated_batch_code"], incident["expected_affected_batch_codes"],
            incident["scenario"], incident["expected_review_required"],
        )
    contexts = datasets.connection.execute(
        "SELECT context_id, organization_id FROM investigation_contexts WHERE status='active'"
    ).fetchall()
    runs = []
    for context in contexts:
        context_id = UUID(context["context_id"])
        RegistryGraphProjectionService(datasets, registry, graph).project_registry_entities(context_id)
        events = graph.list_event_envelopes(UUID(context["organization_id"]))
        summary = RegistryGraphProjectionService(datasets, registry, graph).project_events(context_id, events)
        replay = RegistryGraphProjectionService(datasets, registry, graph).project_events(context_id, events)
        runs.append({"context_id": str(context_id), "events": len(events), "edges": summary.edges_upserted, "replayed": replay.replayed})
    result = {
        **base, **entity_counts, "events": store_events, "incidents": incident_count,
        "graph_nodes": graph.connection.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0],
        "graph_edges": graph.connection.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0],
        "projection_runs": datasets.connection.execute("SELECT COUNT(*) FROM projection_runs").fetchone()[0],
        "genealogy_runs": runs,
    }
    graph.close(); registry.close(); datasets.close()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=Path("data/generated/bangladesh_pharma_smoke"))
    parser.add_argument("--database", type=Path, default=Path("data/local/scei_genealogy_smoke.db"))
    args = parser.parse_args()
    args.database.parent.mkdir(parents=True, exist_ok=True)
    print(json.dumps(enrich_and_project(args.dataset, args.database), indent=2))
