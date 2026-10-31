# Bangladesh synthetic dataset

`generate_bangladesh_dataset.py` creates deterministic, streaming JSONL evidence for the pharmaceutical batch-contamination domain.

The records are intentionally synthetic and should never be represented as real company, regulatory, shipment, or patient data.

## Profiles

| Profile | Organizations | Suppliers | Facilities | Materials | Products | Batches | Incidents |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `smoke` | 4 | 16 | 12 | 24 | 40 | 250 | 3 |
| `large` | 60 | 420 | 260 | 220 | 700 | 50,000 | 40 |

The large profile emits approximately hundreds of thousands of event, document, and evidence-record rows. Output is gzip-compressed by default and is generated on demand rather than committed to Git.

## Generate

From the repository root:

```powershell
python scripts/generate_bangladesh_dataset.py --profile smoke
python scripts/generate_bangladesh_dataset.py --profile large --output data/generated/bangladesh_pharma_large
```

Use `--seed` to reproduce a dataset exactly. Use `--no-compression` only when inspecting files manually.

## Deliberate messiness

The generator includes:

- Bangladesh districts, divisions, DGDA-style synthetic licenses, and Chattogram/Dhaka/Benapole/Burimari logistics routes;
- domestic and cross-border suppliers;
- multiple source-system vocabularies;
- legacy, case-variant, OCR-corrupted, truncated, and punctuation-free batch identifiers;
- scanned-document flags and Bangla/English/mixed-language metadata;
- batch transformations, quantity variation, quality status, expiry dates, and multi-hop lineage;
- incident records containing expected downstream impact for evaluation;
- explicit synthetic flags and ground-truth canonical values.
