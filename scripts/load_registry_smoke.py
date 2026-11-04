"""Load only batch entities from one generated dataset version into a fresh registry DB."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from scei.datasets.models import ValidationReport
from scei.datasets.sqlite import SQLiteDatasetRepository, file_sha256
from scei.registry.models import (
    CanonicalEntity,
    EntityAlias,
    EntityDatasetMembership,
    EntityIdentifier,
    EntityLifecycle,
    RegistryEntityType,
    RegistryProvenance,
)
from scei.registry.sqlite import SQLiteRegistryRepository
from scei.resolution.normalization import normalize_identifier, normalize_name


def rows(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            yield line_number, json.loads(line)


def load(dataset: Path, database: Path) -> dict[str, object]:
    manifest_path = dataset / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dataset_id = uuid5(NAMESPACE_URL, f"scei:dataset:{manifest['dataset_name']}")
    version_id = uuid5(dataset_id, f"{manifest['dataset_version']}:{manifest['seed']}:{manifest['profile']}")
    workspace_id = UUID(int=0)
    dataset_repo = SQLiteDatasetRepository(database)
    registry = SQLiteRegistryRepository(database)
    dataset_repo.register_dataset(
        dataset_id, manifest["dataset_name"], "pharmaceutical_batch_contamination", True
    )
    dataset_repo.register_version(
        version_id,
        dataset_id,
        manifest["dataset_version"],
        "dataset-manifest-v1",
        file_sha256(manifest_path),
        "scei-generator-0.1",
        None,
        manifest["seed"],
        manifest["profile"],
    )
    registered_files: dict[str, str] = {}
    for path in sorted(dataset.glob("*.jsonl.gz")):
        checksum = file_sha256(path)
        registered_files[path.name] = checksum
        dataset_repo.register_file(
            uuid5(version_id, path.name), version_id, path.name, str(path), checksum,
            path.stat().st_size, None, "gzip", "jsonl-v1",
        )
    organizations = list(rows(dataset / "organizations.jsonl.gz"))
    for _, organization in organizations:
        dataset_repo.add_organization(
            UUID(organization["organization_id"]),
            organization["organization_code"],
            organization["legal_name"],
        )
    contexts: dict[UUID, UUID] = {}
    loads: dict[UUID, UUID] = {}
    for _, organization in organizations:
        organization_id = UUID(organization["organization_id"])
        context_id = uuid5(version_id, f"context:{organization_id}")
        dataset_repo.create_context(context_id, organization_id, workspace_id, version_id)
        contexts[organization_id] = context_id
        loads[organization_id] = dataset_repo.begin_load(version_id, context_id)
    count = 0
    for row_number, batch in rows(dataset / "batches.jsonl.gz"):
        organization_id = UUID(organizations[batch["organization_index"]][1]["organization_id"])
        entity_id = UUID(batch["batch_id"])
        provenance_id = uuid5(entity_id, f"provenance:{version_id}")
        registry.add_entity(
            CanonicalEntity(
                entity_id=entity_id,
                organization_id=organization_id,
                entity_type=RegistryEntityType.BATCH,
                canonical_name=batch["batch_code"],
                identity_origin=f"dataset:{version_id}",
                status=EntityLifecycle.ACTIVE,
                attributes={"batch_kind": batch["batch_kind"], "quality_status": batch["quality_status"]},
            )
        )
        registry.add_provenance(
            RegistryProvenance(
                provenance_id=provenance_id,
                organization_id=organization_id,
                entity_id=entity_id,
                dataset_version_id=version_id,
                source_file="batches.jsonl.gz",
                source_row_reference=str(row_number),
            )
        )
        registry.add_identifier(
            EntityIdentifier(
                identifier_id=uuid5(entity_id, "identifier:batch_code"),
                organization_id=organization_id,
                entity_id=entity_id,
                identifier_type="batch_code",
                namespace="synthetic_generator",
                raw_value=batch["batch_code"],
                normalized_value=normalize_identifier(batch["batch_code"]),
                is_primary=True,
            )
        )
        registry.add_alias(
            EntityAlias(
                alias_id=uuid5(entity_id, "alias:legacy_batch_code"),
                organization_id=organization_id,
                entity_id=entity_id,
                alias_type="legacy_batch_code",
                raw_value=batch["legacy_batch_code"],
                normalized_value=normalize_name(batch["legacy_batch_code"]),
                provenance_id=provenance_id,
            )
        )
        registry.add_membership(
            EntityDatasetMembership(
                membership_id=uuid5(entity_id, f"membership:{version_id}"),
                organization_id=organization_id,
                entity_id=entity_id,
                dataset_version_id=version_id,
                source_entity_key=batch["batch_id"],
                source_table="batches",
                source_row_reference=str(row_number),
                provenance_id=provenance_id,
            )
        )
        count += 1
    # Re-read the registered files before validation/activation.  This catches
    # a changed or partially-written source file without activating the
    # corresponding dataset version.
    for logical_name, expected_checksum in registered_files.items():
        actual_checksum = file_sha256(dataset / logical_name)
        if actual_checksum != expected_checksum:
            raise ValueError(f"checksum_mismatch:{logical_name}")
    report = ValidationReport(valid=True, counts={"organizations": len(organizations), "batches": count})
    for load_run_id in loads.values():
        dataset_repo.mark_load_validated(load_run_id, report)
    for context_id in contexts.values():
        dataset_repo.activate_context(context_id)
    result = {
        "database": str(database),
        "dataset_id": str(dataset_id),
        "dataset_version_id": str(version_id),
        "organizations": len(organizations),
        "batches": count,
        "contexts_activated": len(contexts),
        "registry_entities": dataset_repo.count("canonical_entities"),
        "identifiers": dataset_repo.count("entity_identifiers"),
        "aliases": dataset_repo.count("entity_aliases"),
        "memberships": dataset_repo.count("entity_dataset_memberships"),
        "provenance": dataset_repo.count("registry_provenance"),
        "conflicts": dataset_repo.count("identity_conflicts"),
    }
    registry.close()
    dataset_repo.close()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=Path("data/generated/bangladesh_pharma_smoke"))
    parser.add_argument("--database", type=Path, default=Path("data/local/scei_registry_smoke.db"))
    args = parser.parse_args()
    args.database.parent.mkdir(parents=True, exist_ok=True)
    print(json.dumps(load(args.dataset, args.database), indent=2))
