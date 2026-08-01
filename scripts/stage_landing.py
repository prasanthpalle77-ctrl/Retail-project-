"""Stage a generated NovaRetail batch into immutable local Landing."""

from __future__ import annotations

import argparse
import json
from pathlib import Path, PureWindowsPath
from uuid import uuid4

from retail_lakehouse.ingestion import FileRegistry, stage_file


def resolve_reported_source(generated_batch: Path, reported_path: str) -> Path:
    """Resolve a report entry without reading outside the supplied batch directory."""

    batch_root = generated_batch.resolve()
    candidate = Path(reported_path).resolve()
    try:
        candidate.relative_to(batch_root)
    except ValueError:
        candidate = batch_root / PureWindowsPath(reported_path).name
    return candidate


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
        source_path = resolve_reported_source(args.generated_batch, str(file_path))
        if not source_path.is_file():
            raise FileNotFoundError(f"Reported source is not present in batch: {source_path}")
        result = stage_file(
            source_path,
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
    main()
