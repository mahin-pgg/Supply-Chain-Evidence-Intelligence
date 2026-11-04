from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid5

from scei.graph.projection_service import GraphProjectionService, ProjectionSummary
from scei.resolution.models import EntityProfile
from scei.storage.sqlite import SQLiteCanonicalStore


class ResolutionApplicationError(RuntimeError):
    pass


class ResolutionApplicationService:
    """Apply an approved review to mappings and graph projection atomically."""

    def __init__(self, store: SQLiteCanonicalStore) -> None:
        self.store = store

    def apply(self, organization_id: UUID, review_id: UUID, context_id: UUID | None = None) -> dict[str, object]:
        with self.store.application_lock:
            review = self.store.get_resolution_review(organization_id, review_id)
            if review is None:
                raise ResolutionApplicationError("review not found")
            if review["status"] == "applied":
                return self._result(review, 0, None)
            if review["status"] not in {"approved", "application_failed", "applying"}:
                raise ResolutionApplicationError(
                    f"review status {review['status']} cannot be applied"
                )
            applying = self.store.begin_review_application(organization_id, review_id)
            if applying is None:
                raise ResolutionApplicationError("review is already being applied or is not approved")
            try:
                selected_entity_id = applying.get("selected_entity_id")
                if not selected_entity_id:
                    raise ResolutionApplicationError("approved review has no selected entity")
                selected_entity = UUID(str(selected_entity_id))
                if not self.store.entity_belongs_to_tenant(organization_id, selected_entity):
                    raise ResolutionApplicationError("selected entity does not belong to the tenant")
                events = self.store.list_events_referencing_identifier(
                    organization_id, str(applying["observed_value"])
                )
                profiles = self._profiles(organization_id)
                dataset_version_id = None
                if context_id is not None:
                    context = self.store.connection.execute(
                        "SELECT dataset_version_id FROM investigation_contexts WHERE context_id=? AND organization_id=? AND status='active'",
                        (str(context_id), str(organization_id)),
                    ).fetchone()
                    if context is None:
                        raise ResolutionApplicationError("active investigation context not found")
                    dataset_version_id = UUID(context["dataset_version_id"])
                with self.store.batch_write():
                    self.store.add_resolution_mapping(
                        organization_id=organization_id,
                        mapping_id=uuid5(review_id, "mapping"),
                        observed_identifier=str(applying["observed_value"]),
                        field_name=str(applying["field_name"]),
                        entity_type=str(applying["entity_type"]),
                        canonical_entity_id=selected_entity,
                        source_review_id=review_id,
                        reviewer_id=str(applying["reviewer_id"]),
                        decision_at=str(applying["reviewed_at"] or datetime.now(UTC).isoformat()),
                    )
                    summary = GraphProjectionService(self.store, profiles).project(events, context_id, dataset_version_id)
                    self.store.mark_review_applied(organization_id, review_id)
                applied = self.store.get_resolution_review(organization_id, review_id)
                assert applied is not None
                return self._result(applied, len(events), summary)
            except Exception as error:
                self.store.mark_review_application_failed(organization_id, review_id, str(error))
                raise

    def _profiles(self, organization_id: UUID) -> list[EntityProfile]:
        graph_profiles = [
            EntityProfile(
                entity_id=UUID(row["node_id"]),
                organization_id=organization_id,
                entity_type=str(row["node_type"]),
                canonical_name=str(row["label"]),
                identifiers=[str(row["label"])],
            )
            for row in self.store.list_graph_nodes(organization_id)
        ]
        return graph_profiles + self.store.registry_profiles(organization_id)

    @staticmethod
    def _result(
        review: dict[str, object], events_reprojected: int, summary: ProjectionSummary | None
    ) -> dict[str, object]:
        return {
            "review_id": review["review_id"],
            "status": review["status"],
            "events_reprojected": events_reprojected,
            "nodes_written": summary.nodes_written if summary else 0,
            "edges_written": summary.edges_written if summary else 0,
            "projection_gaps": summary.gaps if summary else {},
        }
