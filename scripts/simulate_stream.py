"""Emit a JSON Lines fixture as atomic files for local Structured Streaming."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from retail_lakehouse.streaming import emit_json_microbatches


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_path", type=Path)
    parser.add_argument("output_directory", type=Path)
    parser.add_argument("--records-per-file", type=int, default=10)
    parser.add_argument("--interval-seconds", type=float, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = emit_json_microbatches(
        args.source_path,
        args.output_directory,
        records_per_file=args.records_per_file,
        interval_seconds=args.interval_seconds,
    )
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
