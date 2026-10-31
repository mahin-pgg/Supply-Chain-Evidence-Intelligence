"""Load generated Bangladesh evidence into the local canonical SQLite adapter."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from uuid import UUID

from scei.ingestion.models import EvidenceRecordIngest, EventIngestEnvelope, SourceDocumentIngest
from scei.storage.sqlite import SQLiteCanonicalStore


def rows(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        for line in stream:
            yield json.loads(line)


def load(dataset: Path, database: Path) -> dict[str, int]:
    store = SQLiteCanonicalStore(database)
    organizations = [row["organization_id"] for row in rows(dataset / "organizations.jsonl.gz")]
    counts = {"documents": 0, "evidence": 0, "events": 0}
    document_path = dataset / "documents_and_records.jsonl.gz"
    counts["documents"] = store.add_documents(
        SourceDocumentIngest(organization_id=UUID(organizations[row["organization_index"]]), document_id=UUID(row["document_id"]), document_type=row["document_type"], file_name=row["file_name"], mime_type=row["mime_type"], source_channel=row["source_channel"], received_at=row["received_at"], sha256=row["sha256"], language=row["language"], contains_scan=row["contains_scan"])
        for row in rows(document_path) if "record_id" not in row
    )
    counts["evidence"] = store.add_evidence_records(
        EvidenceRecordIngest(organization_id=UUID(organizations[row["organization_index"]]), record_id=UUID(row["record_id"]), document_id=UUID(row["document_id"]), field_name=row["field_name"], observed_value=row["observed_value"], canonical_value=row["canonical_value"], corruption_type=row["corruption_type"], source_locator=row["source_locator"], gold_reference=row["gold_batch_code"])
        for row in rows(document_path) if "record_id" in row
    )
    counts["events"] = store.add_events(
        EventIngestEnvelope(organization_id=UUID(organizations[row["organization_index"]]), event_id=UUID(row["event_id"]), event_type=row["event_type"], event_time=row["event_time"], idempotency_key=f"{row['source_system']}:{row['event_id']}", source_system=row["source_system"], payload={key: value for key, value in row.items() if key not in {"event_id", "organization_index", "event_type", "event_time", "source_system", "provenance"}}, provenance=[row["provenance"]])
        for row in rows(dataset / "events.jsonl.gz")
    )
    store.close()
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=Path("data/generated/bangladesh_pharma_large"))
    parser.add_argument("--database", type=Path, default=Path("data/local/scei.db"))
    args = parser.parse_args()
    args.database.parent.mkdir(parents=True, exist_ok=True)
    print(json.dumps(load(args.dataset, args.database), indent=2))
