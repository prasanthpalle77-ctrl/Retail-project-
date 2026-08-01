"""Build the deterministic local knowledge index."""

from __future__ import annotations

import argparse
from pathlib import Path

from retail_lakehouse.rag import LexicalIndex, load_documents


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--documents", type=Path, default=Path("data/documents"))
    parser.add_argument("--output", type=Path, default=Path("data/index/knowledge_index.json"))
    args = parser.parse_args()
    chunks = load_documents(args.documents)
    LexicalIndex(chunks).save(args.output)
    print(f"Indexed {len(chunks)} governed chunks at {args.output}")


if __name__ == "__main__":
    main()
