"""Command line interface for the offline UUID migration checker."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .core import build_plan, check_manifest
from .samples import generate_manifest


def _load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def format_summary(report: dict[str, Any], plan: dict[str, Any]) -> str:
    """Render a human-readable summary of report + plan."""
    lines = [
        f"UUID migration check (pymongo {report['pymongo_version']})",
        f"records: {report['summary']['total_records']}  "
        f"status: {report['summary']['status_counts']}  "
        f"collision groups: {report['summary']['collision_groups']}",
        "",
        "Per-record results:",
    ]
    steps = {s["record_id"]: s for s in plan["steps"]}
    for record in report["records"]:
        step = steps[record["record_id"]]
        lines.append(
            f"  {record['record_id']:<10} status={record['status']:<14} action={step['action']}"
        )
        for reason in record["reasons"]:
            lines.append(f"{'':<14}- {reason}")
        for field in record["uuid_fields"]:
            matrix = ", ".join(
                f"{name}={uuid or '(stays Binary)'}"
                for name, uuid in field["decode_matrix"].items()
            )
            lines.append(f"{'':<14}- {field['path']} subtype={field['subtype']}: {matrix}")
            if field["query_bytes"]:
                matching = [
                    name for name, q in field["query_bytes"].items() if q["matches_stored_bytes"]
                ]
                lines.append(
                    f"{'':<16}query for {field['canonical_uuid']} matches stored bytes "
                    f"only when encoded as: {', '.join(matching)}"
                )
    if report["collisions"]:
        lines.append("")
        lines.append("Identity collisions after migration:")
        for collision in report["collisions"]:
            members = ", ".join(
                f"{m['record_id']}:{m['path']} (subtype {m['subtype']})"
                for m in collision["members"]
            )
            lines.append(f"  {collision['canonical_uuid']}: {members}")
    unsafe = report["summary"]["unsafe_records"]
    lines.append("")
    if unsafe:
        lines.append("Records that CANNOT be auto-converted: " + ", ".join(unsafe))
    else:
        lines.append("All records with UUID fields can be auto-converted.")
    hazards = report["summary"]["ejson_hazard_records"]
    if hazards:
        lines.append("Records with Extended JSON round-trip hazards: " + ", ".join(hazards))
    return "\n".join(lines)


def cmd_generate(args: argparse.Namespace) -> int:
    manifest = generate_manifest()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "manifest.json"
    _write_json(path, manifest)
    print(f"wrote {path} ({len(manifest['samples'])} samples)")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    manifest = _load_manifest(Path(args.manifest))
    report = check_manifest(manifest)
    plan = build_plan(report)
    if args.report:
        _write_json(Path(args.report), report)
    if args.plan:
        _write_json(Path(args.plan), plan)
    print(format_summary(report, plan))
    return 1 if report["summary"]["unsafe_records"] else 0


def cmd_demo(args: argparse.Namespace) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    manifest = generate_manifest()
    manifest_path = out / "manifest.json"
    _write_json(manifest_path, manifest)
    report = check_manifest(manifest)
    plan = build_plan(report)
    _write_json(out / "report.json", report)
    _write_json(out / "plan.json", plan)
    print(f"wrote {manifest_path}, {out / 'report.json'}, {out / 'plan.json'}\n")
    print(format_summary(report, plan))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="uuid_migration_check",
        description=(
            "Offline checker for migrating UUID fields from legacy BSON "
            "representations to STANDARD. Never connects to a database."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="write a demo manifest of labelled BSON samples")
    gen.add_argument("--out", default="uuid_check_out")
    gen.set_defaults(func=cmd_generate)
    chk = sub.add_parser("check", help="check a manifest and print a summary")
    chk.add_argument("--manifest", required=True)
    chk.add_argument("--report", help="write the full JSON report here")
    chk.add_argument("--plan", help="write the migration plan JSON here")
    chk.set_defaults(func=cmd_check)
    demo = sub.add_parser("demo", help="generate samples, check them, write report + plan")
    demo.add_argument("--out", default="uuid_check_out")
    demo.set_defaults(func=cmd_demo)
    return parser


def main(argv: list[str | None] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
