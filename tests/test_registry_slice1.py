from datetime import UTC, datetime
from sqlite3 import IntegrityError
from uuid import UUID, uuid4, uuid5

import pytest

from scei.datasets.models import ValidationReport
from scei.datasets.sqlite import SQLiteDatasetRepository
from scei.registry.models import (
    CanonicalEntity,
    EntityAlias,
    EntityIdentifier,
    RegistryEntityType,
    RegistryProvenance,
)
from scei.registry.sqlite import SQLiteRegistryRepository
from scei.resolution.normalization import normalize_identifier, normalize_name

TENANT = UUID("00000000-0000-0000-0000-000000000001")
OTHER_TENANT = UUID("00000000-0000-0000-0000-000000000002")
DATASET = UUID("00000000-0000-0000-0000-000000000010")
VERSION = UUID("00000000-0000-0000-0000-000000000011")
WORKSPACE = UUID(int=0)


def setup_registry(tmp_path):
    path = tmp_path / "registry.db"
    dataset = SQLiteDatasetRepository(path)
    registry = SQLiteRegistryRepository(path)
    for tenant, code in [(TENANT, "TENANT-A"), (OTHER_TENANT, "TENANT-B")]:
        dataset.add_organization(tenant, code, code)
    dataset.register_dataset(DATASET, "test-dataset", "pharma", True)
    dataset.register_version(
        VERSION, DATASET, "v1", "test-schema-v1", "manifest", "test-generator", None, 7, "smoke"
    )
    return dataset, registry


def add_batch(registry, entity_id, tenant=TENANT, code="BATCH-001", start=None, end=None):
    provenance_id = uuid5(entity_id, "provenance")
    registry.add_entity(
        CanonicalEntity(
            entity_id=entity_id,
            organization_id=tenant,
            entity_type=RegistryEntityType.BATCH,
            canonical_name=code,
            identity_origin="test",
        )
    )
    registry.add_provenance(
        RegistryProvenance(
            provenance_id=provenance_id,
            organization_id=tenant,
            entity_id=entity_id,
            dataset_version_id=VERSION,
            source_file="batches.jsonl.gz",
            source_row_reference="1",
        )
    )
    registry.add_identifier(
        EntityIdentifier(
            identifier_id=uuid5(entity_id, "code"), organization_id=tenant, entity_id=entity_id,
            identifier_type="batch_code", namespace="MES", raw_value=code,
            normalized_value=normalize_identifier(code), is_primary=True,
            valid_from=start, valid_to=end,
        )
    )
    registry.add_alias(
        EntityAlias(
            alias_id=uuid5(entity_id, "legacy"), organization_id=tenant, entity_id=entity_id,
            alias_type="legacy_batch_code", raw_value=f"LOT-{code}",
            normalized_value=normalize_name(f"LOT-{code}"), provenance_id=provenance_id,
        )
    )
    return provenance_id


def test_registry_candidates_work_without_graph_nodes(tmp_path):
    dataset, registry = setup_registry(tmp_path)
    entity_id = uuid4()
    add_batch(registry, entity_id, code="BATCH-001")
    exact = registry.find_candidates(TENANT, "batch", normalize_identifier("BATCH-001"))
    alias = registry.find_candidates(TENANT, "batch", normalize_name("LOT-BATCH-001"))
    assert [candidate.entity.entity_id for candidate in exact] == [entity_id]
    assert alias[0].matched_alias is not None
    dataset.close()
    registry.close()


def test_cross_tenant_identity_isolated_by_service_and_composite_fk(tmp_path):
    dataset, registry = setup_registry(tmp_path)
    entity_id = uuid4()
    add_batch(registry, entity_id, TENANT, "BATCH-001")
    assert registry.find_candidates(OTHER_TENANT, "batch", normalize_identifier("BATCH-001")) == []
    with pytest.raises(IntegrityError):
        registry.add_identifier(
            EntityIdentifier(
                identifier_id=uuid4(), organization_id=OTHER_TENANT, entity_id=entity_id,
                identifier_type="batch_code", namespace="MES", raw_value="BATCH-001",
                normalized_value="batch001",
            )
        )
    dataset.close()
    registry.close()


def test_duplicate_active_identifier_creates_conflict(tmp_path):
    dataset, registry = setup_registry(tmp_path)
    add_batch(registry, uuid4(), code="DUP-001")
    add_batch(registry, uuid4(), code="DUP-001")
    assert registry.connection.execute("SELECT COUNT(*) FROM identity_conflicts").fetchone()[0] == 1
    dataset.close()
    registry.close()


def test_historical_identifier_reuse_is_allowed_for_non_overlapping_windows(tmp_path):
    dataset, registry = setup_registry(tmp_path)
    first = datetime(2020, 1, 1, tzinfo=UTC)
    boundary = datetime(2021, 1, 1, tzinfo=UTC)
    second_end = datetime(2022, 1, 1, tzinfo=UTC)
    add_batch(registry, uuid4(), code="REUSED-001", start=first, end=boundary)
    add_batch(registry, uuid4(), code="REUSED-001", start=boundary, end=second_end)
    assert registry.connection.execute("SELECT COUNT(*) FROM identity_conflicts").fetchone()[0] == 0
    candidates = registry.find_candidates(
        TENANT, "batch", normalize_identifier("REUSED-001"), as_of=datetime(2021, 6, 1, tzinfo=UTC)
    )
    assert len(candidates) == 1
    dataset.close()
    registry.close()


def test_failed_load_cannot_activate_and_historical_context_remains(tmp_path):
    dataset, registry = setup_registry(tmp_path)
    context = uuid4()
    dataset.create_context(context, TENANT, WORKSPACE, VERSION)
    load_run = dataset.begin_load(VERSION, context)
    dataset.mark_load_validated(load_run, ValidationReport(valid=False, errors=["checksum_mismatch"]))
    with pytest.raises(ValueError, match="validated_load_required"):
        dataset.activate_context(context)
    assert dataset.get_context(context)["status"] == "staged"
    dataset.close()
    registry.close()


def test_projection_snapshot_metadata_is_append_only_and_context_scoped(tmp_path):
    dataset, registry = setup_registry(tmp_path)
    context = uuid4()
    dataset.create_context(context, TENANT, WORKSPACE, VERSION)
    load_run = dataset.begin_load(VERSION, context)
    dataset.mark_load_validated(load_run, ValidationReport(valid=True))
    dataset.activate_context(context)
    dataset.connection.execute(
        """INSERT INTO projection_runs
           (projection_run_id, context_id, dataset_version_id, load_run_id,
            projection_version, configuration_checksum, status, node_count, edge_count,
            validation_report_json, completed_at)
           VALUES (?, ?, ?, ?, ?, ?, 'completed', 0, 0, '{}', CURRENT_TIMESTAMP)""",
        (str(uuid4()), str(context), str(VERSION), str(load_run), "v1", "config-sha"),
    )
    dataset.connection.commit()
    with pytest.raises(IntegrityError):
        dataset.connection.execute(
            """INSERT INTO projection_runs
               (projection_run_id, context_id, dataset_version_id, load_run_id,
                projection_version, configuration_checksum, status)
               VALUES (?, ?, ?, ?, ?, ?, 'completed')""",
            (str(uuid4()), str(context), str(VERSION), str(load_run), "v1", "config-sha"),
        )
    dataset.close()
    registry.close()
