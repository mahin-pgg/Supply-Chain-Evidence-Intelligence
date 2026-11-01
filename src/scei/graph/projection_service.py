from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from uuid import UUID, uuid5

from scei.graph.projector import GenealogyProjector
from scei.ingestion.models import EventIngestEnvelope
from scei.resolution.models import EntityProfile, ResolutionDecision, ResolutionState
from scei.resolution.resolver import EntityResolver
from scei.storage.sqlite import SQLiteCanonicalStore


@dataclass(frozen=True)
class ProjectionSummary:
    events_seen: int
    events_projected: int
    nodes_written: int
    edges_written: int
    gaps: dict[str, int]


class GraphProjectionService:
    """Reproducibly project canonical events into the validated graph."""

    BINDING_FIELDS = (
        "batch_code",
        "source_batch_code",
        "destination_batch_code",
        "input_batch_code",
        "product_code",
    )

    def __init__(self, store: SQLiteCanonicalStore, profiles: list[EntityProfile]) -> None:
        self.store = store
        self.resolver = EntityResolver(profiles)
        self.projector = GenealogyProjector(store)

    def project(self, events: list[EventIngestEnvelope]) -> ProjectionSummary:
        gaps = Counter[str]()
        projected = nodes = edges = 0
        with self.store.batch_write():
            for event in events:
                bindings = self._bindings(event)
                result = self.projector.project_event(event, bindings)
                nodes += result.nodes_written
                edges += result.edges_written
                if result.nodes_written or result.edges_written:
                    projected += 1
                for gap in result.gaps:
                    gaps[gap.split(":", 1)[0]] += 1
        return ProjectionSummary(
            events_seen=len(events),
            events_projected=projected,
            nodes_written=nodes,
            edges_written=edges,
            gaps=dict(sorted(gaps.items())),
        )

    def _bindings(self, event: EventIngestEnvelope):
        bindings = {}
        for field in self.BINDING_FIELDS:
            value = event.payload.get(field)
            if value:
                entity_type = "product" if field == "product_code" else "batch"
                mapping = self.store.get_resolution_mapping(
                    event.organization_id, str(value), field, entity_type
                )
                if mapping is not None:
                    decision = ResolutionDecision(
                        organization_id=event.organization_id,
                        entity_type=entity_type,
                        observed_value=str(value),
                        selected_entity_id=UUID(str(mapping["canonical_entity_id"])),
                        state=ResolutionState.VERIFIED,
                        score=1.0,
                        review_required=False,
                        rationale=f"Applied review mapping {mapping['source_review_id']}.",
                    )
                else:
                    decision = self.resolver.resolve(
                        event.organization_id, entity_type, str(value), allow_fuzzy=False
                    )
                bindings[str(value)] = decision
                if decision.selected_entity_id is None:
                    self.store.add_resolution_review(
                        organization_id=event.organization_id,
                        review_id=uuid5(event.event_id, f"resolution:{field}:{value}"),
                        event_id=event.event_id,
                        field_name=field,
                        entity_type=entity_type,
                        observed_value=str(value),
                        candidates=[candidate.model_dump(mode="json") for candidate in decision.candidates],
                        rationale=decision.rationale,
                    )
        return bindings
