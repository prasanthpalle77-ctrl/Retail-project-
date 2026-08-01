"""Stage a generated NovaRetail batch into immutable local Landing."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from uuid import uuid4

from retail_lakehouse.ingestion import FileRegistry, stage_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("generated_batch", type=Path)
    parser.add_argument("--landing-root", type=Path, default=Path("data/landing"))
    parser.add_argument("--run-id", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report_path = args.generated_batch / "generation_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    registry = FileRegistry(args.landing_root / "_control" / "processed_files.json")
    run_id = args.run_id or str(uuid4())
    results = []
    for source_name, file_path in sorted(report["files"].items()):
        result = stage_file(
            Path(file_path),
            args.landing_root,
            registry,
            source_name=source_name,
            source_system="NOVARETAIL_SYNTHETIC",
            batch_id=report["batch_id"],
            run_id=run_id,
        )
        results.append(
            {
                "source_name": source_name,
                "status": result.status.value,
                "landing_file": result.manifest.landing_file_path,
                "manifest": result.manifest_path,
            }
        )
    print(json.dumps({"run_id": run_id, "files": results}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
