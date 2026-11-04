from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from scei.datasets.models import ValidationReport
from scei.datasets.schema import SCHEMA


class SQLiteDatasetRepository:
    """Dataset lifecycle adapter with staged validation and atomic activation."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.executescript(SCHEMA)

    def close(self) -> None:
        self.connection.close()

    def register_dataset(self, dataset_id: UUID, name: str, domain: str, synthetic: bool) -> None:
        self.connection.execute(
            "INSERT INTO dataset_registry(dataset_id,dataset_name,domain,synthetic) VALUES (?,?,?,?)",
            (str(dataset_id), name, domain, int(synthetic)),
        )
        self.connection.commit()

    def register_version(
        self,
        dataset_version_id: UUID,
        dataset_id: UUID,
        version_label: str,
        schema_version: str,
        manifest_sha256: str,
        generator_version: str | None,
        generator_commit: str | None,
        seed: int | None,
        profile: str | None,
    ) -> None:
        self.connection.execute(
            """INSERT INTO dataset_versions
               (dataset_version_id,dataset_id,version_label,schema_version,manifest_sha256,
                generator_version,generator_commit,seed,profile,status)
               VALUES (?,?,?,?,?,?,?,?,?,'staged')""",
            (
                str(dataset_version_id), str(dataset_id), version_label, schema_version,
                manifest_sha256, generator_version, generator_commit, seed, profile,
            ),
        )
        self.connection.commit()

    def register_file(
        self, dataset_file_id: UUID, dataset_version_id: UUID, logical_name: str,
        path: str, sha256: str, byte_size: int, row_count: int | None, compression: str | None,
        schema_fingerprint: str,
    ) -> None:
        self.connection.execute(
            """INSERT INTO dataset_files
               (dataset_file_id,dataset_version_id,logical_name,path,sha256,byte_size,row_count,
                compression,schema_fingerprint)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                str(dataset_file_id), str(dataset_version_id), logical_name, path, sha256,
                byte_size, row_count, compression, schema_fingerprint,
            ),
        )
        self.connection.commit()

    def create_context(
        self, context_id: UUID, organization_id: UUID, workspace_id: UUID, dataset_version_id: UUID
    ) -> None:
        self.connection.execute(
            """INSERT INTO investigation_contexts
               (context_id,organization_id,workspace_id,dataset_version_id,status)
               VALUES (?,?,?,?, 'staged')""",
            (str(context_id), str(organization_id), str(workspace_id), str(dataset_version_id)),
        )
        self.connection.commit()

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

    def begin_load(self, dataset_version_id: UUID, context_id: UUID) -> UUID:
        load_run_id = uuid4()
        self.connection.execute(
            "INSERT INTO load_runs(load_run_id,dataset_version_id,context_id,status) VALUES (?,?,?,'staged')",
            (str(load_run_id), str(dataset_version_id), str(context_id)),
        )
        self.connection.commit()
        return load_run_id

    def mark_load_validated(self, load_run_id: UUID, report: ValidationReport) -> None:
        status = "validated" if report.valid else "failed"
        self.connection.execute(
            """UPDATE load_runs SET status=?, completed_at=CURRENT_TIMESTAMP,
               validation_report_json=?, error_message=? WHERE load_run_id=?""",
            (status, report.model_dump_json(), "; ".join(report.errors) or None, str(load_run_id)),
        )
        self.connection.execute(
            """UPDATE dataset_versions SET status=? WHERE dataset_version_id=(
               SELECT dataset_version_id FROM load_runs WHERE load_run_id=?)""",
            (status, str(load_run_id)),
        )
        self.connection.commit()

    def activate_context(self, context_id: UUID) -> None:
        context = self.connection.execute(
            "SELECT * FROM investigation_contexts WHERE context_id = ?", (str(context_id),)
        ).fetchone()
        if context is None:
            raise ValueError("context_not_found")
        load = self.connection.execute(
            "SELECT * FROM load_runs WHERE context_id=? ORDER BY started_at DESC LIMIT 1",
            (str(context_id),),
        ).fetchone()
        if load is None or load["status"] != "validated":
            raise ValueError("validated_load_required")
        try:
            self.connection.execute("BEGIN")
            self.connection.execute(
                """UPDATE investigation_contexts SET status='retired', retired_at=CURRENT_TIMESTAMP
                   WHERE organization_id=? AND workspace_id=? AND status='active'""",
                (context["organization_id"], context["workspace_id"]),
            )
            self.connection.execute(
                "UPDATE investigation_contexts SET status='active', activated_at=CURRENT_TIMESTAMP WHERE context_id=?",
                (str(context_id),),
            )
            self.connection.execute(
                "UPDATE dataset_versions SET status='active', activated_at=CURRENT_TIMESTAMP WHERE dataset_version_id=?",
                (context["dataset_version_id"],),
            )
            self.connection.execute(
                "UPDATE load_runs SET status='active' WHERE load_run_id=?", (load["load_run_id"],)
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def get_context(self, context_id: UUID) -> dict[str, object] | None:
        row = self.connection.execute(
            "SELECT * FROM investigation_contexts WHERE context_id=?", (str(context_id),)
        ).fetchone()
        return dict(row) if row else None

    def get_active_load(self, context_id: UUID) -> dict[str, object] | None:
        row = self.connection.execute(
            "SELECT * FROM load_runs WHERE context_id=? AND status='active' ORDER BY completed_at DESC LIMIT 1",
            (str(context_id),),
        ).fetchone()
        return dict(row) if row else None

    def start_projection_run(
        self, context_id: UUID, projection_version: str, configuration_checksum: str,
    ) -> dict[str, object]:
        context = self.get_context(context_id)
        load = self.get_active_load(context_id)
        if context is None or context["status"] != "active":
            raise ValueError("active_context_required")
        if load is None:
            raise ValueError("active_load_required")
        projection_run_id = uuid5(
            NAMESPACE_URL, f"scei:projection:{context_id}:{projection_version}:{configuration_checksum}"
        )
        self.connection.execute(
            """INSERT OR IGNORE INTO projection_runs
               (projection_run_id, context_id, dataset_version_id, load_run_id,
                projection_version, configuration_checksum, status)
               VALUES (?, ?, ?, ?, ?, ?, 'running')""",
            (str(projection_run_id), str(context_id), context["dataset_version_id"],
             load["load_run_id"], projection_version, configuration_checksum),
        )
        self.connection.commit()
        row = self.connection.execute(
            "SELECT * FROM projection_runs WHERE projection_run_id=?", (str(projection_run_id),)
        ).fetchone()
        return dict(row)

    def complete_projection_run(
        self, projection_run_id: UUID, node_count: int, edge_count: int,
        validation_report: dict[str, object],
    ) -> dict[str, object]:
        self.connection.execute(
            """UPDATE projection_runs SET status='completed', node_count=?, edge_count=?,
               validation_report_json=?, completed_at=CURRENT_TIMESTAMP
               WHERE projection_run_id=? AND status='running'""",
            (node_count, edge_count, json.dumps(validation_report, separators=(",", ":")),
             str(projection_run_id)),
        )
        self.connection.commit()
        row = self.connection.execute(
            "SELECT * FROM projection_runs WHERE projection_run_id=?", (str(projection_run_id),)
        ).fetchone()
        return dict(row)

    def fail_projection_run(self, projection_run_id: UUID, error: str) -> None:
        self.connection.execute(
            """UPDATE projection_runs SET status='failed', validation_report_json=?
               WHERE projection_run_id=? AND status='running'""",
            (json.dumps({"errors": [error[:2000]]}), str(projection_run_id)),
        )
        self.connection.commit()

    def count(self, table: str) -> int:
        allowed = {
            "canonical_entities", "entity_identifiers", "entity_aliases",
            "entity_dataset_memberships", "registry_provenance", "identity_conflicts",
            "dataset_registry", "dataset_versions", "dataset_files", "load_runs",
            "investigation_contexts", "projection_runs",
        }
        if table not in allowed:
            raise ValueError("unsupported_table")
        return int(self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
