from __future__ import annotations

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS registry_organizations (
    organization_id TEXT PRIMARY KEY,
    organization_code TEXT NOT NULL,
    legal_name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS registry_workspaces (
    organization_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    workspace_name TEXT NOT NULL,
    PRIMARY KEY (organization_id, workspace_id),
    FOREIGN KEY (organization_id) REFERENCES registry_organizations(organization_id)
);

CREATE TABLE IF NOT EXISTS dataset_registry (
    dataset_id TEXT PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    domain TEXT NOT NULL,
    synthetic INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dataset_versions (
    dataset_version_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    version_label TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    generator_version TEXT,
    generator_commit TEXT,
    seed INTEGER,
    profile TEXT,
    manifest_sha256 TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at TEXT,
    retired_at TEXT,
    UNIQUE (dataset_id, version_label),
    FOREIGN KEY (dataset_id) REFERENCES dataset_registry(dataset_id)
);

CREATE TABLE IF NOT EXISTS dataset_files (
    dataset_file_id TEXT PRIMARY KEY,
    dataset_version_id TEXT NOT NULL,
    logical_name TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    row_count INTEGER,
    compression TEXT,
    schema_fingerprint TEXT NOT NULL,
    UNIQUE (dataset_version_id, logical_name),
    FOREIGN KEY (dataset_version_id) REFERENCES dataset_versions(dataset_version_id)
);

CREATE TABLE IF NOT EXISTS investigation_contexts (
    context_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    dataset_version_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at TEXT,
    retired_at TEXT,
    UNIQUE (organization_id, context_id),
    FOREIGN KEY (organization_id, workspace_id)
        REFERENCES registry_workspaces(organization_id, workspace_id),
    FOREIGN KEY (dataset_version_id) REFERENCES dataset_versions(dataset_version_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_context_per_workspace
    ON investigation_contexts(organization_id, workspace_id)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS load_runs (
    load_run_id TEXT PRIMARY KEY,
    dataset_version_id TEXT NOT NULL,
    context_id TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    inserted_counts_json TEXT NOT NULL DEFAULT '{}',
    validation_report_json TEXT NOT NULL DEFAULT '{}',
    error_message TEXT,
    FOREIGN KEY (dataset_version_id) REFERENCES dataset_versions(dataset_version_id),
    FOREIGN KEY (context_id) REFERENCES investigation_contexts(context_id)
);

-- Projection runs are append-only snapshots.  The graph adapter will populate
-- this table in a later slice; keeping the contract here prevents a completed
-- projection from being treated as mutable context state.
CREATE TABLE IF NOT EXISTS projection_runs (
    projection_run_id TEXT PRIMARY KEY,
    context_id TEXT NOT NULL,
    dataset_version_id TEXT NOT NULL,
    load_run_id TEXT NOT NULL,
    projection_version TEXT NOT NULL,
    configuration_checksum TEXT NOT NULL,
    status TEXT NOT NULL,
    node_count INTEGER NOT NULL DEFAULT 0,
    edge_count INTEGER NOT NULL DEFAULT 0,
    validation_report_json TEXT NOT NULL DEFAULT '{}',
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    UNIQUE (context_id, projection_version, configuration_checksum),
    FOREIGN KEY (context_id) REFERENCES investigation_contexts(context_id),
    FOREIGN KEY (dataset_version_id) REFERENCES dataset_versions(dataset_version_id),
    FOREIGN KEY (load_run_id) REFERENCES load_runs(load_run_id)
);

CREATE TABLE IF NOT EXISTS canonical_entities (
    organization_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    identity_origin TEXT NOT NULL,
    status TEXT NOT NULL,
    superseded_by_entity_id TEXT,
    valid_from TEXT,
    valid_to TEXT,
    attributes_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (organization_id, entity_id),
    UNIQUE (organization_id, entity_id),
    FOREIGN KEY (organization_id) REFERENCES registry_organizations(organization_id),
    FOREIGN KEY (organization_id, superseded_by_entity_id)
        REFERENCES canonical_entities(organization_id, entity_id)
);

CREATE TABLE IF NOT EXISTS entity_identifiers (
    identifier_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    identifier_type TEXT NOT NULL,
    namespace TEXT NOT NULL,
    raw_value TEXT NOT NULL,
    normalized_value TEXT NOT NULL,
    is_primary INTEGER NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    status TEXT NOT NULL,
    FOREIGN KEY (organization_id, entity_id)
        REFERENCES canonical_entities(organization_id, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_entity_identifier_lookup
    ON entity_identifiers(organization_id, identifier_type, namespace, normalized_value);

CREATE TABLE IF NOT EXISTS entity_aliases (
    alias_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    alias_type TEXT NOT NULL,
    raw_value TEXT NOT NULL,
    normalized_value TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    status TEXT NOT NULL,
    provenance_id TEXT NOT NULL,
    FOREIGN KEY (organization_id, entity_id)
        REFERENCES canonical_entities(organization_id, entity_id),
    FOREIGN KEY (organization_id, provenance_id)
        REFERENCES registry_provenance(organization_id, provenance_id)
);

CREATE INDEX IF NOT EXISTS idx_entity_alias_lookup
    ON entity_aliases(organization_id, normalized_value);

CREATE TABLE IF NOT EXISTS entity_dataset_memberships (
    membership_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    dataset_version_id TEXT NOT NULL,
    source_entity_key TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_row_reference TEXT,
    membership_status TEXT NOT NULL,
    provenance_id TEXT NOT NULL,
    UNIQUE (organization_id, dataset_version_id, source_table, source_entity_key),
    FOREIGN KEY (organization_id, entity_id)
        REFERENCES canonical_entities(organization_id, entity_id),
    FOREIGN KEY (dataset_version_id) REFERENCES dataset_versions(dataset_version_id),
    FOREIGN KEY (organization_id, provenance_id)
        REFERENCES registry_provenance(organization_id, provenance_id)
);

CREATE TABLE IF NOT EXISTS registry_provenance (
    provenance_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    dataset_version_id TEXT,
    source_file TEXT NOT NULL,
    source_row_reference TEXT,
    locator_json TEXT NOT NULL DEFAULT '{}',
    extraction_method TEXT NOT NULL,
    confidence REAL NOT NULL,
    UNIQUE (organization_id, provenance_id),
    FOREIGN KEY (organization_id, entity_id)
        REFERENCES canonical_entities(organization_id, entity_id),
    FOREIGN KEY (dataset_version_id) REFERENCES dataset_versions(dataset_version_id)
);

CREATE TABLE IF NOT EXISTS identity_conflicts (
    conflict_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL,
    identifier_type TEXT NOT NULL,
    namespace TEXT NOT NULL,
    normalized_value TEXT NOT NULL,
    entity_ids_json TEXT NOT NULL,
    conflict_type TEXT NOT NULL,
    status TEXT NOT NULL,
    detected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (organization_id) REFERENCES registry_organizations(organization_id)
);
"""
