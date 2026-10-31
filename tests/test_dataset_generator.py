import gzip
import json
from pathlib import Path

from scripts.generate_bangladesh_dataset import Settings, generate


def read_jsonl(path: Path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        yield from (json.loads(line) for line in stream)


def test_smoke_dataset_is_deterministic(tmp_path):
    first = generate(Settings(seed=7, profile="smoke", output=tmp_path / "first", compressed=True))
    second = generate(Settings(seed=7, profile="smoke", output=tmp_path / "second", compressed=True))
    assert first["counts"] == second["counts"]
    assert next(read_jsonl(tmp_path / "first" / "batches.jsonl.gz")) == next(read_jsonl(tmp_path / "second" / "batches.jsonl.gz"))


def test_dataset_has_bangladesh_context_and_ground_truth(tmp_path):
    manifest = generate(Settings(seed=9, profile="smoke", output=tmp_path / "dataset", compressed=True))
    assert manifest["synthetic"] is True
    assert manifest["geography"]["primary_country"] == "BD"
    organizations = list(read_jsonl(tmp_path / "dataset" / "organizations.jsonl.gz"))
    incidents = list(read_jsonl(tmp_path / "dataset" / "incidents.jsonl.gz"))
    records = list(read_jsonl(tmp_path / "dataset" / "documents_and_records.jsonl.gz"))
    assert organizations and all(row["country"] == "BD" for row in organizations)
    assert incidents and all(row["expected_review_required"] for row in incidents)
    assert any(row.get("field_name") == "batch_identifier" for row in records)


def test_batch_identifiers_are_unique(tmp_path):
    generate(Settings(seed=11, profile="smoke", output=tmp_path / "dataset", compressed=True))
    batches = list(read_jsonl(tmp_path / "dataset" / "batches.jsonl.gz"))
    identifiers = [row["batch_code"] for row in batches]
    assert len(identifiers) == len(set(identifiers))
    assert all(identifier.startswith("BATCH-BD-") for identifier in identifiers)
