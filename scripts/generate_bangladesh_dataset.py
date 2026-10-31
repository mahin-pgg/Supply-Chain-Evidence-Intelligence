"""Generate deterministic, Bangladesh-oriented synthetic supply-chain evidence.

The output is synthetic and must never be presented as operational or real company data.
The generator is streaming so large profiles can be produced without keeping all rows in RAM.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


PROFILE_SIZES = {
    "smoke": {"organizations": 4, "suppliers": 16, "facilities": 12, "materials": 24, "products": 40, "batches": 250, "incidents": 3},
    "large": {"organizations": 60, "suppliers": 420, "facilities": 260, "materials": 220, "products": 700, "batches": 50_000, "incidents": 40},
}

DISTRICTS = [
    ("Dhaka", "Dhaka"), ("Gazipur", "Dhaka"), ("Narayanganj", "Dhaka"), ("Savar", "Dhaka"),
    ("Chattogram", "Chattogram"), ("Cumilla", "Chattogram"), ("Mymensingh", "Mymensingh"),
    ("Rajshahi", "Rajshahi"), ("Khulna", "Khulna"), ("Sylhet", "Sylhet"),
    ("Rangpur", "Rangpur"), ("Bogura", "Rajshahi"),
]
PORTS = [
    ("Chattogram Port", "Chattogram", "BDCGP"), ("Dhaka Airport Cargo Village", "Dhaka", "BDDAC"),
    ("Benapole Land Port", "Jashore", "BDBEN"), ("Burimari Land Port", "Lalmonirhat", "BDBUR"),
]
MATERIAL_NAMES = [
    "Paracetamol API", "Amoxicillin Trihydrate", "Azithromycin Dihydrate", "Omeprazole Pellets",
    "Metformin Hydrochloride", "Amlodipine Besylate", "Atorvastatin Calcium", "Esomeprazole Magnesium",
    "Sodium Chloride USP", "Microcrystalline Cellulose", "Magnesium Stearate", "Povidone K-30",
    "Lactose Monohydrate", "Starch 1500", "Titanium Dioxide",
]
PRODUCT_FORMS = ["tablet", "capsule", "oral suspension", "injection", "dry powder"]
CORRUPTIONS = ["case_variant", "punctuation_removed", "transposed_character", "legacy_prefix", "ocr_substitution", "missing_suffix", "none"]


@dataclass(frozen=True)
class Settings:
    seed: int
    profile: str
    output: Path
    compressed: bool

    @property
    def sizes(self) -> dict[str, int]:
        return PROFILE_SIZES[self.profile]


def stable_uuid(seed: str) -> str:
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"


def corrupt_identifier(value: str, rng: random.Random, corruption: str | None = None) -> tuple[str, str]:
    corruption = corruption or rng.choices(CORRUPTIONS, weights=[12, 10, 8, 6, 4, 5, 55])[0]
    if corruption == "case_variant":
        return value.lower(), corruption
    if corruption == "punctuation_removed":
        return value.replace("-", ""), corruption
    if corruption == "transposed_character" and len(value) > 5:
        position = rng.randrange(2, len(value) - 1)
        chars = list(value)
        chars[position], chars[position + 1] = chars[position + 1], chars[position]
        return "".join(chars), corruption
    if corruption == "legacy_prefix":
        return f"LEG-{value}", corruption
    if corruption == "ocr_substitution":
        return value.replace("0", "O").replace("1", "I"), corruption
    if corruption == "missing_suffix":
        return value[:-2], corruption
    return value, "none"


def write_jsonl(path: Path, rows: Iterator[dict], compressed: bool) -> int:
    opener = gzip.open if compressed else open
    with opener(path, "wt", encoding="utf-8") as stream:
        count = 0
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            count += 1
    return count


def organizations(settings: Settings) -> Iterator[dict]:
    for index in range(settings.sizes["organizations"]):
        district, division = DISTRICTS[index % len(DISTRICTS)]
        yield {"organization_id": stable_uuid(f"org:{settings.seed}:{index}"), "organization_code": f"BD-PH-{index + 1:04d}", "legal_name": f"{['Bengal', 'Delta', 'Padma', 'Meghna', 'Jamuna'][index % 5]} Therapeutics Bangladesh {index + 1:03d} Ltd.", "country": "BD", "registered_district": district, "division": division, "tenant_status": "active", "synthetic": True}


def suppliers(settings: Settings) -> Iterator[dict]:
    countries = [("BD", "Bangladesh"), ("IN", "India"), ("CN", "China"), ("SG", "Singapore"), ("DE", "Germany")]
    for index in range(settings.sizes["suppliers"]):
        country_code, country = countries[index % len(countries)]
        district, division = DISTRICTS[index % len(DISTRICTS)]
        yield {"supplier_id": stable_uuid(f"supplier:{settings.seed}:{index}"), "supplier_code": f"SUP-{country_code}-{index + 1:05d}", "legal_name": f"{['Bengal', 'Eastern', 'Narayanganj', 'Chattogram', 'Padma'][index % 5]} Pharma Chemicals {index + 1:04d} Ltd.", "country_code": country_code, "country": country, "district": district if country_code == "BD" else None, "division": division if country_code == "BD" else None, "registration_number": f"DGDA-{country_code}-{2018 + index % 7}-{index + 1:05d}", "synthetic": True}


def facilities(settings: Settings) -> Iterator[dict]:
    kinds = ["supplier_plant", "warehouse", "manufacturing_plant", "quality_lab", "distributor"]
    for index in range(settings.sizes["facilities"]):
        district, division = DISTRICTS[index % len(DISTRICTS)]
        facility_kind = kinds[index % len(kinds)]
        yield {"facility_id": stable_uuid(f"facility:{settings.seed}:{index}"), "facility_code": f"FAC-BD-{index + 1:05d}", "name": f"{district} {facility_kind.replace('_', ' ').title()} {index + 1:03d}", "facility_kind": facility_kind, "country_code": "BD", "district": district, "division": division, "latitude": round(20.7 + (index * 0.017) % 3.8, 6), "longitude": round(88.0 + (index * 0.023) % 3.0, 6), "gmp_license": f"DGDA-GMP-{2019 + index % 6}-{index + 1:05d}", "synthetic": True}


def materials(settings: Settings) -> Iterator[dict]:
    for index in range(settings.sizes["materials"]):
        name = MATERIAL_NAMES[index % len(MATERIAL_NAMES)]
        yield {"material_id": stable_uuid(f"material:{settings.seed}:{index}"), "material_code": f"MAT-{index + 1:05d}", "name": f"{name} Grade {['USP', 'BP', 'EP'][index % 3]}", "material_class": "api" if "API" in name or index % 4 == 0 else "excipient", "unit": "kg", "cas_number": f"{1000 + index % 8999}-{index % 90:02d}-{index % 9}", "synthetic": True}


def products(settings: Settings) -> Iterator[dict]:
    for index in range(settings.sizes["products"]):
        yield {"product_id": stable_uuid(f"product:{settings.seed}:{index}"), "product_code": f"BD-MED-{index + 1:05d}", "brand_name": f"Nirapod {index + 1:03d}", "dosage_form": PRODUCT_FORMS[index % len(PRODUCT_FORMS)], "strength": f"{[5, 10, 20, 40, 500][index % 5]} mg", "market": "Bangladesh", "regulatory_authority": "DGDA", "synthetic": True}


def batch_rows(settings: Settings) -> Iterator[dict]:
    rng = random.Random(settings.seed)
    material_count, facility_count, product_count = settings.sizes["materials"], settings.sizes["facilities"], settings.sizes["products"]
    start = date(2021, 1, 1)
    for index in range(settings.sizes["batches"]):
        batch_kind = "raw" if index < material_count * 2 else ("api" if index % 5 else "intermediate")
        parent_index = index - rng.randint(1, min(index, 25)) if index > material_count * 2 else None
        parent_id = f"BATCH-BD-{parent_index + 1:08d}" if parent_index is not None else None
        quantity = round(rng.uniform(40, 2400), 3)
        produced = start + timedelta(days=rng.randint(0, 1700))
        yield {"batch_id": stable_uuid(f"batch:{settings.seed}:{index}"), "batch_code": f"BATCH-BD-{index + 1:08d}", "legacy_batch_code": f"LOT-{['DHK', 'GAZ', 'CTG', 'NAR'][index % 4]}-{20000 + index}", "batch_kind": batch_kind, "material_index": index % material_count, "parent_batch_code": parent_id, "product_index": index % product_count if index % 7 == 0 else None, "origin_facility_index": index % facility_count, "quantity_kg": quantity, "manufactured_date": produced.isoformat(), "expiry_date": (produced + timedelta(days=rng.choice([365, 540, 730, 1095]))).isoformat(), "quality_status": rng.choices(["approved", "released", "quarantined", "rejected"], weights=[55, 32, 9, 4])[0], "synthetic": True}


def event_rows(settings: Settings) -> Iterator[dict]:
    rng = random.Random(settings.seed + 11)
    facility_count = settings.sizes["facilities"]
    for index, batch in enumerate(batch_rows(settings)):
        batch_code, date_value = batch["batch_code"], batch["manufactured_date"]
        yield {"event_id": stable_uuid(f"event:{settings.seed}:{index}:receive"), "organization_index": index % settings.sizes["organizations"], "event_type": "RECEIVE", "event_time": f"{date_value}T08:30:00+06:00", "batch_code": batch_code, "facility_index": batch["origin_facility_index"], "quantity": {"value": batch["quantity_kg"], "unit": "kg"}, "source_system": rng.choice(["SAP_BD_EXPORT", "WMS_DHAKA", "SUPPLIER_CSV", "EDI_856"]), "provenance": {"document_id": stable_uuid(f"doc:{settings.seed}:{index}:certificate"), "locator": "page=1;table=batch_summary;row=1"}, "synthetic": True}
        if batch["parent_batch_code"]:
            yield {"event_id": stable_uuid(f"event:{settings.seed}:{index}:transform"), "organization_index": index % settings.sizes["organizations"], "event_type": "TRANSFORM", "event_time": f"{date_value}T14:15:00+06:00", "source_batch_code": batch["parent_batch_code"], "destination_batch_code": batch_code, "facility_index": (batch["origin_facility_index"] + 1) % facility_count, "quantity": {"value": round(batch["quantity_kg"] * rng.uniform(0.88, 0.99), 3), "unit": "kg"}, "source_system": rng.choice(["MES_GAZIPUR", "MES_SAVAR", "ERP_MANUFACTURING"]), "provenance": {"document_id": stable_uuid(f"doc:{settings.seed}:{index}:manifest"), "locator": "sheet=Events;row=4"}, "synthetic": True}
        if batch["product_index"] is not None:
            yield {"event_id": stable_uuid(f"event:{settings.seed}:{index}:produce"), "organization_index": index % settings.sizes["organizations"], "event_type": "PRODUCE", "event_time": f"{date_value}T18:45:00+06:00", "input_batch_code": batch_code, "product_index": batch["product_index"], "facility_index": (batch["origin_facility_index"] + 2) % facility_count, "quantity": {"value": round(batch["quantity_kg"] * 1000 / 0.5, 3), "unit": "units"}, "source_system": "MES_FINISHED_GOODS", "provenance": {"document_id": stable_uuid(f"doc:{settings.seed}:{index}:production"), "locator": "page=2;table=yield"}, "synthetic": True}


def document_rows(settings: Settings) -> Iterator[dict]:
    rng = random.Random(settings.seed + 22)
    for index, batch in enumerate(batch_rows(settings)):
        batch_code = batch["batch_code"]
        observed, corruption = corrupt_identifier(batch_code, rng)
        document_id = stable_uuid(f"doc:{settings.seed}:{index}:certificate")
        yield {"document_id": document_id, "organization_index": index % settings.sizes["organizations"], "document_type": "batch_certificate", "file_name": f"supplier_{index % settings.sizes['suppliers'] + 1:05d}_batch_{index + 1:08d}.pdf", "mime_type": "application/pdf", "source_channel": rng.choice(["supplier_portal", "email_attachment", "sftp", "manual_upload"]), "received_at": f"{batch['manufactured_date']}T09:00:00+06:00", "sha256": hashlib.sha256(f"synthetic-document:{settings.seed}:{index}".encode()).hexdigest(), "language": rng.choices(["en", "bn", "mixed"], weights=[55, 20, 25])[0], "contains_scan": rng.random() < 0.27, "synthetic": True}
        yield {"document_id": stable_uuid(f"doc:{settings.seed}:{index}:manifest"), "organization_index": index % settings.sizes["organizations"], "document_type": rng.choice(["shipping_manifest", "invoice", "quality_inspection"]), "file_name": f"shipment_{index + 1:08d}.{rng.choice(['xlsx', 'csv', 'docx'])}", "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "source_channel": rng.choice(["erp_export", "edi", "email_attachment"]), "received_at": f"{batch['manufactured_date']}T16:30:00+06:00", "sha256": hashlib.sha256(f"synthetic-manifest:{settings.seed}:{index}".encode()).hexdigest(), "language": "en", "contains_scan": False, "synthetic": True}
        yield {"record_id": stable_uuid(f"record:{settings.seed}:{index}:batch"), "organization_index": index % settings.sizes["organizations"], "document_id": document_id, "field_name": "batch_identifier", "observed_value": observed, "canonical_value": batch_code, "corruption_type": corruption, "source_locator": {"page": 1, "bounding_box": [120, 220, 840, 570]}, "gold_batch_code": batch_code, "synthetic": True}


def incident_rows(settings: Settings) -> Iterator[dict]:
    batch_count = settings.sizes["batches"]
    step = max(1, batch_count // settings.sizes["incidents"])
    for index in range(settings.sizes["incidents"]):
        contaminated_index = min(batch_count - 1, index * step + step // 3)
        affected = [contaminated_index + offset for offset in range(0, min(18, batch_count - contaminated_index), 3)]
        yield {"incident_id": f"INC-BD-{settings.seed}-{index + 1:04d}", "tenant_index": index % settings.sizes["organizations"], "severity": "high" if index % 3 else "critical", "contaminated_batch_code": f"BATCH-BD-{contaminated_index + 1:08d}", "expected_affected_batch_codes": [f"BATCH-BD-{item + 1:08d}" for item in affected], "expected_review_required": True, "scenario": "potential_api_contamination", "synthetic": True}


def generate(settings: Settings) -> dict:
    if settings.output.exists():
        shutil.rmtree(settings.output)
    settings.output.mkdir(parents=True, exist_ok=True)
    suffix = ".jsonl.gz" if settings.compressed else ".jsonl"
    counts = {
        "organizations": write_jsonl(settings.output / f"organizations{suffix}", organizations(settings), settings.compressed),
        "suppliers": write_jsonl(settings.output / f"suppliers{suffix}", suppliers(settings), settings.compressed),
        "facilities": write_jsonl(settings.output / f"facilities{suffix}", facilities(settings), settings.compressed),
        "materials": write_jsonl(settings.output / f"materials{suffix}", materials(settings), settings.compressed),
        "products": write_jsonl(settings.output / f"products{suffix}", products(settings), settings.compressed),
        "batches": write_jsonl(settings.output / f"batches{suffix}", batch_rows(settings), settings.compressed),
        "events": write_jsonl(settings.output / f"events{suffix}", event_rows(settings), settings.compressed),
        "documents_and_records": write_jsonl(settings.output / f"documents_and_records{suffix}", document_rows(settings), settings.compressed),
        "incidents": write_jsonl(settings.output / f"incidents{suffix}", incident_rows(settings), settings.compressed),
    }
    manifest = {"dataset_name": "bangladesh_pharma_supply_chain_synthetic", "dataset_version": "0.1.0", "synthetic": True, "warning": "Synthetic data for engineering and evaluation only; not real organizations or operational records.", "seed": settings.seed, "profile": settings.profile, "generated_at": datetime.now(timezone.utc).isoformat(), "counts": counts, "files_are_gzip": settings.compressed, "corruption_types": CORRUPTIONS, "geography": {"primary_country": "BD", "ports": PORTS, "districts": [item[0] for item in DISTRICTS]}}
    (settings.output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def parse_args() -> Settings:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=sorted(PROFILE_SIZES), default="smoke")
    parser.add_argument("--seed", type=int, default=20261031)
    parser.add_argument("--output", type=Path, default=Path("data/generated/bangladesh_pharma"))
    parser.add_argument("--no-compression", action="store_true")
    args = parser.parse_args()
    return Settings(args.seed, args.profile, args.output, not args.no_compression)


if __name__ == "__main__":
    result = generate(parse_args())
    print(json.dumps(result, indent=2))
