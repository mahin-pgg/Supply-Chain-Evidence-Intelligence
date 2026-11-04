from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from uuid import UUID

from scei.datasets.sqlite import SQLiteDatasetRepository
from scei.graph.models import GraphNode
from scei.registry.sqlite import SQLiteRegistryRepository
from scei.storage.sqlite import SQLiteCanonicalStore


@dataclass(frozen=True)
class RegistryProjectionSummary:
    projection_run_id: UUID
    context_id: UUID
    dataset_version_id: UUID
    entities_seen: int
    nodes_upserted: int
    replayed: bool


class RegistryGraphProjectionService:
    """Project registry identities into the existing tenant-scoped graph.

    This first slice intentionally projects batch nodes only. Event edge
    projection remains delegated to the existing genealogy projector until
    products and the remaining registry entity types are loaded.
    """

    def __init__(
        self,
        dataset_repository: SQLiteDatasetRepository,
        registry_repository: SQLiteRegistryRepository,
        graph_store: SQLiteCanonicalStore,
    ) -> None:
        self.datasets = dataset_repository
        self.registry = registry_repository
        self.graph = graph_store

    def project_batches(
        self,
        context_id: UUID,
        projection_version: str = "registry-batch-v1",
        configuration: dict[str, object] | None = None,
    ) -> RegistryProjectionSummary:
        configuration = configuration or {"entity_types": ["batch"], "source": "registry"}
        checksum = hashlib.sha256(
            json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        run = self.datasets.start_projection_run(context_id, projection_version, checksum)
        context = self.datasets.get_context(context_id)
        assert context is not None
        dataset_version_id = UUID(str(context["dataset_version_id"]))
        run_id = UUID(str(run["projection_run_id"]))
        if run["status"] == "completed":
            return RegistryProjectionSummary(
                projection_run_id=run_id,
                context_id=context_id,
                dataset_version_id=dataset_version_id,
                entities_seen=int(run["node_count"]),
                nodes_upserted=0,
                replayed=True,
            )

        entities = self.registry.list_entities_for_dataset(
            UUID(str(context["organization_id"])), dataset_version_id, entity_type="batch"
        )
        try:
            with self.graph.batch_write():
                for entity in entities:
                    self.graph.upsert_graph_node(
                        GraphNode(
                            organization_id=entity.organization_id,
                            node_id=entity.entity_id,
                            node_type=entity.entity_type.value,
                            label=entity.canonical_name,
                            attributes={
                                **entity.attributes,
                                "identity_origin": entity.identity_origin,
                                "registry_entity_id": str(entity.entity_id),
                            },
                        ),
                        context_id=context_id,
                        dataset_version_id=dataset_version_id,
                        projection_run_id=run_id,
                    )
            self.datasets.complete_projection_run(
                run_id,
                node_count=len(entities),
                edge_count=0,
                validation_report={"valid": True, "entity_type": "batch"},
            )
        except Exception as exc:
            self.datasets.fail_projection_run(run_id, str(exc))
            raise
        return RegistryProjectionSummary(
            projection_run_id=run_id,
            context_id=context_id,
            dataset_version_id=dataset_version_id,
            entities_seen=len(entities),
            nodes_upserted=len(entities),
            replayed=False,
        )
