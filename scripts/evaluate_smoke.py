"""Print deterministic evaluation metrics for an isolated SCEI database."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from uuid import UUID

from scei.evaluation import evaluate_organization
from scei.storage.sqlite import SQLiteCanonicalStore



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, default=Path("data/local/scei_phase6_smoke.db"))
    parser.add_argument("--organization", required=True, type=UUID)
    parser.add_argument("--context", type=UUID)
    parser.add_argument("--node", type=UUID)
    args = parser.parse_args()
    store = SQLiteCanonicalStore(args.database)
    print(json.dumps(evaluate_organization(store, args.organization, args.node, args.context).as_dict(), indent=2))
    store.close()
