"""Project registered batch entities into a fresh graph smoke database."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from uuid import UUID

from scei.datasets.sqlite import SQLiteDatasetRepository
from scei.graph.registry_projection import RegistryGraphProjectionService
from scei.registry.sqlite import SQLiteRegistryRepository
from scei.storage.sqlite import SQLiteCanonicalStore


def project(database: Path) -> dict[str, object]:
    datasets = SQLiteDatasetRepository(database)
    registry = SQLiteRegistryRepository(database)
    graph = SQLiteCanonicalStore(database)
    contexts = datasets.connection.execute(
        "SELECT context_id FROM investigation_contexts WHERE status='active' ORDER BY context_id"
    ).fetchall()
    summaries = []
    for row in contexts:
        context_id = UUID(row["context_id"])
        first = RegistryGraphProjectionService(datasets, registry, graph).project_batches(context_id)
        replay = RegistryGraphProjectionService(datasets, registry, graph).project_batches(context_id)
        summaries.append(
            {
                "context_id": str(context_id),
                "projection_run_id": str(first.projection_run_id),
                "dataset_version_id": str(first.dataset_version_id),
                "entities_seen": first.entities_seen,
                "replayed": replay.replayed,
            }
        )
    result = {
        "database": str(database),
        "contexts": summaries,
        "graph_nodes": graph.connection.execute("SELECT COUNT(*) FROM graph_nodes").fetchone()[0],
        "graph_edges": graph.connection.execute("SELECT COUNT(*) FROM graph_edges").fetchone()[0],
        "projection_runs": datasets.connection.execute("SELECT COUNT(*) FROM projection_runs").fetchone()[0],
        "completed_projection_runs": datasets.connection.execute(
            "SELECT COUNT(*) FROM projection_runs WHERE status='completed'"
        ).fetchone()[0],
    }
    graph.close()
    registry.close()
    datasets.close()
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=Path("data/local/scei_registry_projection_smoke.db"))
    args = parser.parse_args()
    print(json.dumps(project(args.database), indent=2))
