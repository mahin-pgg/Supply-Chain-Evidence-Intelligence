from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from scei.graph.projector import GenealogyProjector
from scei.ingestion.models import EventIngestEnvelope
from scei.resolution.models import EntityProfile
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
                bindings[str(value)] = self.resolver.resolve(
                    event.organization_id, entity_type, str(value), allow_fuzzy=False
                )
        return bindings
