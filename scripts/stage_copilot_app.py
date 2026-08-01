"""Assemble the self-contained Databricks Apps deployment directory."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wheel", type=Path)
    return parser.parse_args()


def stage_app(root: Path, output: Path, wheel: Path | None = None) -> dict[str, object]:
    """Copy app files, governed evidence, and the project wheel into one directory."""
    output.mkdir(parents=True, exist_ok=True)
    app_source = root / "apps" / "retail_copilot"
    for filename in ("app.py", "app.yaml", "requirements.txt"):
        shutil.copy2(app_source / filename, output / filename)

    config_target = output / "configs"
    config_target.mkdir(exist_ok=True)
    shutil.copy2(root / "configs" / "rag_sql_templates.yml", config_target)

    document_target = output / "data" / "documents"
    document_target.mkdir(parents=True, exist_ok=True)
    documents = sorted((root / "data" / "documents").glob("*.md"))
    for document in documents:
        shutil.copy2(document, document_target / document.name)

    selected_wheel = wheel or _latest_wheel(root / "dist")
    shutil.copy2(selected_wheel, output / selected_wheel.name)
    manifest = {
        "output": str(output.resolve()),
        "wheel": selected_wheel.name,
        "documents": len(documents),
        "files": sorted(
            str(path.relative_to(output)) for path in output.rglob("*") if path.is_file()
        ),
    }
    (output / "deployment_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    return manifest


def _latest_wheel(directory: Path) -> Path:
    wheels = sorted(
        directory.glob("novaretail_lakehouse-*.whl"), key=lambda path: path.stat().st_mtime
    )
    if not wheels:
        raise FileNotFoundError("Build the project wheel before staging the app.")
    return wheels[-1]


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    print(json.dumps(stage_app(root, args.output, args.wheel), indent=2))


if __name__ == "__main__":
    main()
