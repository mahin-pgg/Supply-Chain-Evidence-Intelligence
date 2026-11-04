from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid5

from scei.datasets.schema import SCHEMA
from scei.registry.models import (
    CanonicalEntity,
    EntityAlias,
    EntityCandidate,
    EntityDatasetMembership,
    EntityIdentifier,
    EntityLifecycle,
    IdentityConflict,
    RegistryProvenance,
)
from scei.resolution.models import EntityProfile


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


class SQLiteRegistryRepository:
    """SQLite adapter; registry business rules remain in the repository contract."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.executescript(SCHEMA)

    def close(self) -> None:
        self.connection.close()

    def add_organization(self, organization_id: UUID, code: str, legal_name: str) -> None:
        self.connection.execute(
            "INSERT OR IGNORE INTO registry_organizations VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
            (str(organization_id), code, legal_name),
        )
        self.connection.execute(
            "INSERT OR IGNORE INTO registry_workspaces VALUES (?, ?, ?)",
            (str(organization_id), str(UUID(int=0)), "Default workspace"),
        )
        self.connection.commit()

    def add_entity(self, entity: CanonicalEntity) -> None:
        self.connection.execute(
            """INSERT INTO canonical_entities
               (organization_id, entity_id, entity_type, canonical_name, identity_origin,
                status, superseded_by_entity_id, valid_from, valid_to, attributes_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(entity.organization_id), str(entity.entity_id), entity.entity_type.value,
                entity.canonical_name, entity.identity_origin, entity.status.value,
                str(entity.superseded_by_entity_id) if entity.superseded_by_entity_id else None,
                entity.valid_from.isoformat() if entity.valid_from else None,
                entity.valid_to.isoformat() if entity.valid_to else None,
                _json(entity.attributes),
            ),
        )
        self.connection.commit()

    def add_identifier(self, identifier: EntityIdentifier) -> None:
        conflicts = self.connection.execute(
            """SELECT entity_id, valid_from, valid_to FROM entity_identifiers
               WHERE organization_id = ? AND identifier_type = ? AND namespace = ?
                 AND normalized_value = ? AND status = 'active'""",
            (
                str(identifier.organization_id), identifier.identifier_type, identifier.namespace,
                identifier.normalized_value,
            ),
        ).fetchall()
        if any(self._intervals_overlap(row, identifier) for row in conflicts):
            entity_ids = {str(row["entity_id"]) for row in conflicts}
            entity_ids.add(str(identifier.entity_id))
            conflict = IdentityConflict(
                conflict_id=uuid5(identifier.identifier_id, "active-identifier-conflict"),
                organization_id=identifier.organization_id,
                identifier_type=identifier.identifier_type,
                namespace=identifier.namespace,
                normalized_value=identifier.normalized_value,
                entity_ids=[UUID(item) for item in sorted(entity_ids)],
                conflict_type="duplicate_active_identifier",
            )
            self.add_conflict(conflict)
        self.connection.execute(
            """INSERT INTO entity_identifiers
               (identifier_id, organization_id, entity_id, identifier_type, namespace,
                raw_value, normalized_value, is_primary, valid_from, valid_to, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(identifier.identifier_id), str(identifier.organization_id), str(identifier.entity_id),
                identifier.identifier_type, identifier.namespace, identifier.raw_value,
                identifier.normalized_value, int(identifier.is_primary),
                identifier.valid_from.isoformat() if identifier.valid_from else None,
                identifier.valid_to.isoformat() if identifier.valid_to else None,
                identifier.status.value,
            ),
        )
        self.connection.commit()

    def add_alias(self, alias: EntityAlias) -> None:
        self.connection.execute(
            """INSERT INTO entity_aliases
               (alias_id, organization_id, entity_id, alias_type, raw_value, normalized_value,
                valid_from, valid_to, status, provenance_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(alias.alias_id), str(alias.organization_id), str(alias.entity_id), alias.alias_type,
                alias.raw_value, alias.normalized_value,
                alias.valid_from.isoformat() if alias.valid_from else None,
                alias.valid_to.isoformat() if alias.valid_to else None, alias.status.value,
                str(alias.provenance_id),
            ),
        )
        self.connection.commit()

    def add_membership(self, membership: EntityDatasetMembership) -> None:
        self.connection.execute(
            """INSERT INTO entity_dataset_memberships
               (membership_id, organization_id, entity_id, dataset_version_id, source_entity_key,
                source_table, source_row_reference, membership_status, provenance_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(membership.membership_id), str(membership.organization_id), str(membership.entity_id),
                str(membership.dataset_version_id), membership.source_entity_key, membership.source_table,
                membership.source_row_reference, membership.membership_status, str(membership.provenance_id),
            ),
        )
        self.connection.commit()

    def add_provenance(self, provenance: RegistryProvenance) -> None:
        self.connection.execute(
            """INSERT INTO registry_provenance
               (provenance_id, organization_id, entity_id, dataset_version_id, source_file,
                source_row_reference, locator_json, extraction_method, confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(provenance.provenance_id), str(provenance.organization_id), str(provenance.entity_id),
                str(provenance.dataset_version_id) if provenance.dataset_version_id else None,
                provenance.source_file, provenance.source_row_reference, _json(provenance.locator),
                provenance.extraction_method, provenance.confidence,
            ),
        )
        self.connection.commit()

    def add_conflict(self, conflict: IdentityConflict) -> None:
        self.connection.execute(
            """INSERT OR IGNORE INTO identity_conflicts
               (conflict_id, organization_id, identifier_type, namespace, normalized_value,
                entity_ids_json, conflict_type, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(conflict.conflict_id), str(conflict.organization_id), conflict.identifier_type,
                conflict.namespace, conflict.normalized_value, _json([str(x) for x in conflict.entity_ids]),
                conflict.conflict_type, conflict.status,
            ),
        )
        self.connection.commit()

    def get_entity(self, organization_id: UUID, entity_id: UUID) -> CanonicalEntity | None:
        row = self.connection.execute(
            "SELECT * FROM canonical_entities WHERE organization_id = ? AND entity_id = ?",
            (str(organization_id), str(entity_id)),
        ).fetchone()
        if row is None:
            return None
        return CanonicalEntity(
            entity_id=UUID(row["entity_id"]), organization_id=UUID(row["organization_id"]),
            entity_type=row["entity_type"], canonical_name=row["canonical_name"],
            identity_origin=row["identity_origin"], status=row["status"],
            superseded_by_entity_id=UUID(row["superseded_by_entity_id"])
            if row["superseded_by_entity_id"] else None,
            valid_from=datetime.fromisoformat(row["valid_from"]) if row["valid_from"] else None,
            valid_to=datetime.fromisoformat(row["valid_to"]) if row["valid_to"] else None,
            attributes=json.loads(row["attributes_json"]),
        )

    def list_entities_for_dataset(
        self, organization_id: UUID, dataset_version_id: UUID,
        entity_type: str | None = None,
    ) -> list[CanonicalEntity]:
        query = """SELECT e.* FROM canonical_entities e
                   JOIN entity_dataset_memberships m
                     ON m.organization_id = e.organization_id AND m.entity_id = e.entity_id
                  WHERE e.organization_id = ? AND m.dataset_version_id = ?
                    AND m.membership_status = 'active' AND e.status = 'active'"""
        params: list[object] = [str(organization_id), str(dataset_version_id)]
        if entity_type:
            query += " AND e.entity_type = ?"
            params.append(entity_type)
        query += " ORDER BY e.entity_id"
        return [self.get_entity(organization_id, UUID(row["entity_id"])) for row in self.connection.execute(query, params)]

    def list_resolution_profiles(
        self, organization_id: UUID, dataset_version_id: UUID,
    ) -> list[EntityProfile]:
        entities = self.list_entities_for_dataset(organization_id, dataset_version_id)
        profiles = []
        for entity in entities:
            identifiers = [
                row["raw_value"]
                for row in self.connection.execute(
                    """SELECT raw_value FROM entity_identifiers
                       WHERE organization_id=? AND entity_id=? AND status='active'""",
                    (str(organization_id), str(entity.entity_id)),
                )
            ]
            profiles.append(
                EntityProfile(
                    entity_id=entity.entity_id,
                    organization_id=entity.organization_id,
                    entity_type=entity.entity_type.value,
                    canonical_name=entity.canonical_name,
                    identifiers=identifiers,
                    attributes={k: str(v) for k, v in entity.attributes.items()},
                )
            )
        return profiles

    def find_candidates(
        self, organization_id: UUID, entity_type: str, normalized_value: str,
        identifier_type: str | None = None, namespace: str | None = None,
        as_of: datetime | None = None,
    ) -> list[EntityCandidate]:
        params: list[object] = [str(organization_id), normalized_value]
        query = """SELECT e.*, i.identifier_id, i.identifier_type, i.namespace, i.raw_value,
                          i.normalized_value AS identifier_normalized,
                          i.valid_from AS identifier_valid_from, i.valid_to AS identifier_valid_to
                     FROM entity_identifiers i
                     JOIN canonical_entities e ON e.organization_id = i.organization_id
                                               AND e.entity_id = i.entity_id
                    WHERE i.organization_id = ? AND e.entity_type = ?
                      AND i.normalized_value = ? AND i.status = 'active'"""
        params.insert(1, entity_type)
        if identifier_type:
            query += " AND i.identifier_type = ?"
            params.append(identifier_type)
        if namespace:
            query += " AND i.namespace = ?"
            params.append(namespace)
        rows = self.connection.execute(query, params).fetchall()
        candidates = []
        for row in rows:
            if as_of and not self._valid_at(row, as_of):
                continue
            entity = self.get_entity(organization_id, UUID(row["entity_id"]))
            if entity and entity.status == EntityLifecycle.ACTIVE:
                candidates.append(
                    EntityCandidate(
                        entity=entity,
                        matched_identifier=EntityIdentifier(
                            identifier_id=UUID(row["identifier_id"]), organization_id=organization_id,
                            entity_id=UUID(row["entity_id"]), identifier_type=row["identifier_type"],
                            namespace=row["namespace"], raw_value=row["raw_value"],
                            normalized_value=row["identifier_normalized"],
                        ),
                        score=1.0,
                    )
                )
        alias_rows = self.connection.execute(
            """SELECT e.*, a.alias_id, a.alias_type, a.raw_value, a.normalized_value AS alias_normalized,
                      a.provenance_id, a.valid_from AS alias_valid_from, a.valid_to AS alias_valid_to
                 FROM entity_aliases a
                 JOIN canonical_entities e ON e.organization_id = a.organization_id
                                           AND e.entity_id = a.entity_id
                WHERE a.organization_id = ? AND e.entity_type = ?
                  AND a.normalized_value = ? AND a.status = 'active'""",
            (str(organization_id), entity_type, normalized_value),
        ).fetchall()
        for row in alias_rows:
            if as_of and not self._valid_at(row, as_of):
                continue
            entity = self.get_entity(organization_id, UUID(row["entity_id"]))
            if entity and entity.status == EntityLifecycle.ACTIVE:
                candidates.append(
                    EntityCandidate(
                        entity=entity,
                        matched_alias=EntityAlias(
                            alias_id=UUID(row["alias_id"]), organization_id=organization_id,
                            entity_id=UUID(row["entity_id"]), alias_type=row["alias_type"],
                            raw_value=row["raw_value"], normalized_value=row["alias_normalized"],
                            provenance_id=UUID(row["provenance_id"]),
                        ),
                        score=0.95,
                    )
                )
        unique: dict[UUID, EntityCandidate] = {}
        for candidate in candidates:
            unique.setdefault(candidate.entity.entity_id, candidate)
        return list(unique.values())

    @staticmethod
    def _valid_at(row: sqlite3.Row, value: datetime) -> bool:
        columns = set(row.keys())
        start_value = row["identifier_valid_from"] if "identifier_valid_from" in columns else row["alias_valid_from"]
        end_value = row["identifier_valid_to"] if "identifier_valid_to" in columns else row["alias_valid_to"]
        start = datetime.fromisoformat(start_value) if start_value else None
        end = datetime.fromisoformat(end_value) if end_value else None
        return (start is None or value >= start) and (end is None or value < end)

    @staticmethod
    def _intervals_overlap(row: sqlite3.Row, identifier: EntityIdentifier) -> bool:
        start = datetime.fromisoformat(row["valid_from"]) if row["valid_from"] else None
        end = datetime.fromisoformat(row["valid_to"]) if row["valid_to"] else None
        other_start, other_end = identifier.valid_from, identifier.valid_to
        return (end is None or other_start is None or end > other_start) and (
            other_end is None or start is None or other_end > start
        )
