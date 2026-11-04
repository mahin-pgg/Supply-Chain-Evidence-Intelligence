from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol
from uuid import UUID

from scei.registry.models import (
    CanonicalEntity,
    EntityAlias,
    EntityCandidate,
    EntityDatasetMembership,
    EntityIdentifier,
    IdentityConflict,
    RegistryProvenance,
)


class EntityRegistryRepository(Protocol):
    def add_entity(self, entity: CanonicalEntity) -> None: ...

    def add_identifier(self, identifier: EntityIdentifier) -> None: ...

    def add_alias(self, alias: EntityAlias) -> None: ...

    def add_membership(self, membership: EntityDatasetMembership) -> None: ...

    def add_provenance(self, provenance: RegistryProvenance) -> None: ...

    def add_conflict(self, conflict: IdentityConflict) -> None: ...

    def get_entity(self, organization_id: UUID, entity_id: UUID) -> CanonicalEntity | None: ...

    def find_candidates(
        self,
        organization_id: UUID,
        entity_type: str,
        normalized_value: str,
        identifier_type: str | None = None,
        namespace: str | None = None,
        as_of: datetime | None = None,
    ) -> Sequence[EntityCandidate]: ...


class DatasetLifecycleRepository(Protocol):
    def register_dataset(self, dataset_id: UUID, name: str, domain: str, synthetic: bool) -> None: ...

    def register_version(self, **kwargs: object) -> None: ...

    def register_file(self, **kwargs: object) -> None: ...

    def begin_load(self, dataset_version_id: UUID, context_id: UUID) -> UUID: ...

    def mark_load_validated(self, load_run_id: UUID, report: dict[str, object]) -> None: ...

    def activate_context(self, context_id: UUID) -> None: ...
