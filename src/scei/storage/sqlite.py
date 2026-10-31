from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from uuid import UUID

from scei.graph.models import GraphEdge, GraphNode
from scei.ingestion.models import EventIngestEnvelope, EvidenceRecordIngest, SourceDocumentIngest

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS source_documents (
    organization_id TEXT NOT NULL,
    document_id TEXT NOT NULL,
    document_type TEXT NOT NULL,
    file_name TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    source_channel TEXT NOT NULL,
    received_at TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    language TEXT NOT NULL,
    contains_scan INTEGER NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (organization_id, document_id),
    UNIQUE (organization_id, sha256)
);

CREATE TABLE IF NOT EXISTS evidence_records (
    organization_id TEXT NOT NULL,
    record_id TEXT NOT NULL,
    document_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    observed_value TEXT,
    canonical_value TEXT,
    corruption_type TEXT NOT NULL,
    source_locator_json TEXT NOT NULL,
    gold_reference TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (organization_id, record_id),
    FOREIGN KEY (organization_id, document_id) REFERENCES source_documents (organization_id, document_id)
);

CREATE TABLE IF NOT EXISTS canonical_events (
    organization_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    source_system TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (organization_id, event_id),
    UNIQUE (organization_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_events_tenant_time ON canonical_events (organization_id, event_time);
CREATE INDEX IF NOT EXISTS idx_evidence_tenant_document ON evidence_records (organization_id, document_id);

CREATE TABLE IF NOT EXISTS graph_nodes (
    organization_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    node_type TEXT NOT NULL,
    label TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (organization_id, node_id)
);

CREATE TABLE IF NOT EXISTS graph_edges (
    organization_id TEXT NOT NULL,
    edge_id TEXT NOT NULL,
    source_node_id TEXT NOT NULL,
    target_node_id TEXT NOT NULL,
    edge_type TEXT NOT NULL,
    event_id TEXT NOT NULL,
    relationship_state TEXT NOT NULL,
    quantity_json TEXT,
    provenance_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (organization_id, edge_id),
    UNIQUE (organization_id, event_id, source_node_id, target_node_id, edge_type)
);

CREATE INDEX IF NOT EXISTS idx_graph_edges_tenant_source ON graph_edges (organization_id, source_node_id);
CREATE INDEX IF NOT EXISTS idx_graph_edges_tenant_target ON graph_edges (organization_id, target_node_id);
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


class SQLiteCanonicalStore:
    """Tenant-scoped local adapter for the canonical ingestion store."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)

    def close(self) -> None:
        self.connection.close()

    def add_document(self, document: SourceDocumentIngest) -> bool:
        cursor = self.connection.execute(
            """INSERT OR IGNORE INTO source_documents
            (organization_id, document_id, document_type, file_name, mime_type,
             source_channel, received_at, sha256, language, contains_scan, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(document.organization_id),
                str(document.document_id),
                document.document_type,
                document.file_name,
                document.mime_type,
                document.source_channel,
                document.received_at.isoformat(),
                document.sha256,
                document.language,
                int(document.contains_scan),
                _json(document.metadata),
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def add_evidence(self, record: EvidenceRecordIngest) -> bool:
        cursor = self.connection.execute(
            """INSERT OR IGNORE INTO evidence_records
            (organization_id, record_id, document_id, field_name, observed_value,
             canonical_value, corruption_type, source_locator_json, gold_reference)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(record.organization_id),
                str(record.record_id),
                str(record.document_id),
                record.field_name,
                record.observed_value,
                record.canonical_value,
                record.corruption_type,
                _json(record.source_locator),
                record.gold_reference,
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def add_event(self, event: EventIngestEnvelope) -> bool:
        cursor = self.connection.execute(
            """INSERT OR IGNORE INTO canonical_events
            (organization_id, event_id, event_type, event_time, idempotency_key,
             source_system, payload_json, provenance_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(event.organization_id),
                str(event.event_id),
                event.event_type,
                event.event_time.isoformat(),
                event.idempotency_key,
                event.source_system,
                _json(event.payload),
                _json(event.provenance),
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def add_documents(
        self, documents: Iterable[SourceDocumentIngest], batch_size: int = 1000
    ) -> int:
        rows = []
        inserted = 0
        for document in documents:
            rows.append(
                (
                    str(document.organization_id),
                    str(document.document_id),
                    document.document_type,
                    document.file_name,
                    document.mime_type,
                    document.source_channel,
                    document.received_at.isoformat(),
                    document.sha256,
                    document.language,
                    int(document.contains_scan),
                    _json(document.metadata),
                )
            )
            if len(rows) >= batch_size:
                inserted += self._insert_documents(rows)
                rows = []
        if rows:
            inserted += self._insert_documents(rows)
        return inserted

    def _insert_documents(self, rows: list[tuple[object, ...]]) -> int:
        before = self.connection.total_changes
        self.connection.executemany(
            """INSERT OR IGNORE INTO source_documents
            (organization_id, document_id, document_type, file_name, mime_type, source_channel,
             received_at, sha256, language, contains_scan, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.connection.commit()
        return self.connection.total_changes - before

    def add_evidence_records(
        self, records: Iterable[EvidenceRecordIngest], batch_size: int = 1000
    ) -> int:
        rows = []
        inserted = 0
        for record in records:
            rows.append(
                (
                    str(record.organization_id),
                    str(record.record_id),
                    str(record.document_id),
                    record.field_name,
                    record.observed_value,
                    record.canonical_value,
                    record.corruption_type,
                    _json(record.source_locator),
                    record.gold_reference,
                )
            )
            if len(rows) >= batch_size:
                inserted += self._insert_evidence(rows)
                rows = []
        if rows:
            inserted += self._insert_evidence(rows)
        return inserted

    def _insert_evidence(self, rows: list[tuple[object, ...]]) -> int:
        before = self.connection.total_changes
        self.connection.executemany(
            """INSERT OR IGNORE INTO evidence_records
            (organization_id, record_id, document_id, field_name, observed_value, canonical_value,
             corruption_type, source_locator_json, gold_reference)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.connection.commit()
        return self.connection.total_changes - before

    def add_events(self, events: Iterable[EventIngestEnvelope], batch_size: int = 1000) -> int:
        rows = []
        inserted = 0
        for event in events:
            rows.append(
                (
                    str(event.organization_id),
                    str(event.event_id),
                    event.event_type,
                    event.event_time.isoformat(),
                    event.idempotency_key,
                    event.source_system,
                    _json(event.payload),
                    _json(event.provenance),
                )
            )
            if len(rows) >= batch_size:
                inserted += self._insert_events(rows)
                rows = []
        if rows:
            inserted += self._insert_events(rows)
        return inserted

    def _insert_events(self, rows: list[tuple[object, ...]]) -> int:
        before = self.connection.total_changes
        self.connection.executemany(
            """INSERT OR IGNORE INTO canonical_events
            (organization_id, event_id, event_type, event_time, idempotency_key, source_system, payload_json, provenance_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self.connection.commit()
        return self.connection.total_changes - before

    def count(self, table: str, organization_id: UUID | None = None) -> int:
        if table not in {"source_documents", "evidence_records", "canonical_events"}:
            raise ValueError("unsupported table")
        if organization_id is None:
            return int(self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        return int(
            self.connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE organization_id = ?", (str(organization_id),)
            ).fetchone()[0]
        )

    def list_events(self, organization_id: UUID, limit: int = 100) -> list[dict[str, object]]:
        rows: Iterable[sqlite3.Row] = self.connection.execute(
            "SELECT * FROM canonical_events WHERE organization_id = ? ORDER BY event_time LIMIT ?",
            (str(organization_id), limit),
        )
        return [dict(row) for row in rows]

    def upsert_graph_node(self, node: GraphNode) -> None:
        self.connection.execute(
            """INSERT INTO graph_nodes (organization_id, node_id, node_type, label, attributes_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (organization_id, node_id) DO UPDATE SET label=excluded.label, attributes_json=excluded.attributes_json""",
            (
                str(node.organization_id),
                str(node.node_id),
                node.node_type,
                node.label,
                _json(node.attributes),
            ),
        )
        self.connection.commit()

    def add_graph_edge(self, edge: GraphEdge) -> bool:
        cursor = self.connection.execute(
            """INSERT OR IGNORE INTO graph_edges
            (organization_id, edge_id, source_node_id, target_node_id, edge_type, event_id, relationship_state, quantity_json, provenance_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(edge.organization_id),
                str(edge.edge_id),
                str(edge.source_node_id),
                str(edge.target_node_id),
                edge.edge_type,
                str(edge.event_id),
                edge.relationship_state.value,
                _json(edge.quantity) if edge.quantity else None,
                _json(edge.provenance),
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def list_graph_edges(
        self, organization_id: UUID, source_node_id: UUID | None = None, limit: int = 100
    ) -> list[dict[str, object]]:
        if source_node_id is None:
            rows = self.connection.execute(
                "SELECT * FROM graph_edges WHERE organization_id = ? ORDER BY created_at LIMIT ?",
                (str(organization_id), limit),
            )
        else:
            rows = self.connection.execute(
                "SELECT * FROM graph_edges WHERE organization_id = ? AND source_node_id = ? ORDER BY created_at LIMIT ?",
                (str(organization_id), str(source_node_id), limit),
            )
        return [dict(row) for row in rows]

    def list_graph_nodes(
        self, organization_id: UUID, node_type: str | None = None, limit: int = 10000
    ) -> list[dict[str, object]]:
        if node_type is None:
            rows = self.connection.execute(
                "SELECT * FROM graph_nodes WHERE organization_id = ? LIMIT ?",
                (str(organization_id), limit),
            )
        else:
            rows = self.connection.execute(
                "SELECT * FROM graph_nodes WHERE organization_id = ? AND node_type = ? LIMIT ?",
                (str(organization_id), node_type, limit),
            )
        return [dict(row) for row in rows]
