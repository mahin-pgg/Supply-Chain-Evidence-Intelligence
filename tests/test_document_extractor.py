from uuid import uuid4

from scei.documents.extractor import DocumentExtractor
from scei.documents.models import DocumentKind


def test_text_extraction_preserves_claim_provenance(tmp_path):
    path = tmp_path / "certificate.txt"
    path.write_text("Batch LOT-A-102 received 500 kg on 2026-08-01\nঢাকা facility", encoding="utf-8")
    document = DocumentExtractor().extract(path, uuid4(), uuid4())
    assert document.kind == DocumentKind.TEXT
    assert document.claims[0].field_name == "batch_identifier"
    assert document.claims[0].normalized_value == "lota102"
    assert document.claims[0].locator.paragraph == 1


def test_csv_extraction_preserves_cell_coordinates(tmp_path):
    path = tmp_path / "manifest.csv"
    path.write_text("Batch,Quantity\nBATCH-BD-0001,250 kg\n", encoding="utf-8")
    document = DocumentExtractor().extract(path, uuid4(), uuid4())
    assert document.kind == DocumentKind.CSV
    assert any(claim.field_name == "batch_identifier" for claim in document.claims)
    quantity = next(claim for claim in document.claims if claim.field_name == "quantity")
    assert quantity.locator.row == 2
    assert quantity.locator.column == 2
