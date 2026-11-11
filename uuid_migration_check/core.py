"""Offline checker for migrating UUID fields to the STANDARD BSON representation.

Pure ``bson`` encode/decode analysis: this module never opens a socket or
touches a database.  It consumes BSON samples whose *source representation*
is explicitly labelled, and produces a check report plus a migration plan.

Key rules enforced here:

* subtype-3 (legacy UUID) bytes without a declared source representation are
  **ambiguous** -- the same 16 bytes decode to three different UUIDs under
  PYTHON_LEGACY / JAVA_LEGACY / CSHARP_LEGACY.  They are reported, never
  silently rewritten.
* records whose post-migration identity would collide with another record
  are excluded from automatic conversion.
"""

from __future__ import annotations

from typing import Any, Iterator, Union
from uuid import UUID

import pymongo
from bson import BSON, CodecOptions
from bson.binary import OLD_UUID_SUBTYPE, UUID_SUBTYPE, Binary, UuidRepresentation
from bson.json_util import JSONMode, JSONOptions
from bson.json_util import dumps as ejson_dumps
from bson.json_util import loads as ejson_loads

#: Representation names accepted in manifests, mapped to PyMongo constants.
REPRESENTATIONS = {
    "PYTHON_LEGACY": UuidRepresentation.PYTHON_LEGACY,
    "JAVA_LEGACY": UuidRepresentation.JAVA_LEGACY,
    "CSHARP_LEGACY": UuidRepresentation.CSHARP_LEGACY,
    "STANDARD": UuidRepresentation.STANDARD,
}

_REP_NAMES = {value: name for name, value in REPRESENTATIONS.items()}

_UUID_SUBTYPES = (OLD_UUID_SUBTYPE, UUID_SUBTYPE)

STATUS_KEEP = "keep"  # already STANDARD subtype 4, nothing to do
STATUS_REWRITE = "rewrite"  # legacy subtype 3 with known source: safe to convert
STATUS_AMBIGUOUS = "ambiguous"  # subtype 3 without a declared source
STATUS_LABEL_MISMATCH = "label_mismatch"  # declared source contradicts stored bytes
STATUS_NO_UUID = "no_uuid_fields"
STATUS_ERROR = "error"

BinaryLike = Union[Binary, UUID]


def representation_name(rep: int) -> str:
    """Return the manifest name for a :class:`UuidRepresentation` value."""
    return _REP_NAMES.get(rep, f"UNKNOWN({rep})")


def _codec(rep: int) -> CodecOptions:
    return CodecOptions(uuid_representation=rep)


def walk_binaries(value: Any, path: str = "") -> Iterator[tuple[str, BinaryLike]]:
    """Yield ``(dotted_path, value)`` for every Binary or UUID in a document."""
    if isinstance(value, (Binary, UUID)):
        yield path, value
    elif isinstance(value, dict):
        for key, item in value.items():
            child = f"{path}.{key}" if path else str(key)
            yield from walk_binaries(item, child)
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            child = f"{path}.{index}" if path else str(index)
            yield from walk_binaries(item, child)


def _stored_bytes(value: BinaryLike) -> bytes:
    """Return the 16 payload bytes of a Binary or native UUID."""
    return value.bytes if isinstance(value, UUID) else bytes(value)


def _as_uuid_str(value: BinaryLike, rep: int) -> str | None:
    """Decode *value* to a UUID string under *rep*, or None if not possible."""
    if isinstance(value, UUID):
        return str(value)
    if value.subtype not in _UUID_SUBTYPES or len(value) != 16:
        return None
    if value.subtype == UUID_SUBTYPE:
        rep = UuidRepresentation.STANDARD
    elif rep in (UuidRepresentation.UNSPECIFIED, UuidRepresentation.STANDARD):
        return None  # subtype 3 does not decode to a UUID under STANDARD
    try:
        return str(value.as_uuid(rep))
    except ValueError:
        return None


def decode_matrix(stored: Binary) -> dict[str, str | None]:
    """Show which UUID the stored bytes decode to under each representation.

    ``None`` means the bytes stay a :class:`~bson.binary.Binary` under that
    representation (the driver does not decode them to a native UUID).
    """
    return {name: _as_uuid_str(stored, rep) for name, rep in REPRESENTATIONS.items()}


def query_bytes(canonical: UUID, stored: Binary) -> dict[str, Any]:
    """Encode a point query for *canonical* under every representation.

    A query matches the stored value only when both the binary subtype and
    the payload bytes are identical -- this is why a STANDARD client cannot
    find legacy-stored rows before migration.
    """
    per_rep: dict[str, Any] = {}
    for name, rep in REPRESENTATIONS.items():
        encoded = BSON.encode({"_q": canonical}, codec_options=_codec(rep))
        query_value = BSON(encoded).decode(codec_options=_codec(UuidRepresentation.UNSPECIFIED))[
            "_q"
        ]
        per_rep[name] = {
            "hex": encoded.hex(),
            "subtype": query_value.subtype,
            "matches_stored_bytes": query_value.subtype == stored.subtype
            and bytes(query_value) == bytes(stored),
        }
    return per_rep


def _analyze_uuid_field(path: str, stored: Binary, declared_rep: int | None) -> dict[str, Any]:
    field: dict[str, Any] = {
        "path": path,
        "subtype": stored.subtype,
        "stored_hex": bytes(stored).hex(),
        "decode_matrix": decode_matrix(stored),
        "canonical_uuid": None,
        "query_bytes": None,
        "rewrite": None,
    }
    if declared_rep is not None:
        canonical = _as_uuid_str(stored, declared_rep)
        if canonical is not None:
            field["canonical_uuid"] = canonical
            field["query_bytes"] = query_bytes(UUID(canonical), stored)
    return field


def analyze_record(
    record_id: str,
    kind: str,
    source_name: str | None,
    raw: bytes,
) -> dict[str, Any]:
    """Analyze one BSON sample and return its report entry."""
    record: dict[str, Any] = {
        "record_id": record_id,
        "kind": kind,
        "declared_source": source_name,
        "status": STATUS_ERROR,
        "reasons": [],
        "uuid_fields": [],
        "non_uuid_binaries": [],
        "rewritten_bson_hex": None,
        "rewrite_verified": False,
        "ejson_probes": [],
        "auto_convertible": False,
    }
    if source_name is not None and source_name not in REPRESENTATIONS:
        record["reasons"].append(
            f"unknown source representation label {source_name!r}; "
            f"expected one of {sorted(REPRESENTATIONS)}"
        )
        return record
    declared_rep = REPRESENTATIONS[source_name] if source_name else None

    try:
        doc = BSON(raw).decode(codec_options=_codec(UuidRepresentation.UNSPECIFIED))
    except Exception as exc:  # invalid BSON input
        record["reasons"].append(f"cannot decode BSON sample: {exc}")
        return record

    for path, value in walk_binaries(doc):
        if isinstance(value, Binary) and value.subtype in _UUID_SUBTYPES and len(value) == 16:
            record["uuid_fields"].append(_analyze_uuid_field(path, value, declared_rep))
        elif isinstance(value, Binary):
            record["non_uuid_binaries"].append(
                {"path": path, "subtype": value.subtype, "hex": bytes(value).hex()}
            )

    if not record["uuid_fields"]:
        record["status"] = STATUS_NO_UUID
        record["reasons"].append("no 16-byte subtype-3/4 UUID fields found")
        return record

    subtypes = {f["subtype"] for f in record["uuid_fields"]}

    if declared_rep is None:
        if OLD_UUID_SUBTYPE in subtypes:
            record["status"] = STATUS_AMBIGUOUS
            record["reasons"].append(
                "subtype-3 UUID bytes without a declared source representation: "
                "PYTHON_LEGACY, JAVA_LEGACY and CSHARP_LEGACY decode them to "
                "different UUIDs (see decode_matrix); refusing to guess"
            )
        else:
            record["status"] = STATUS_KEEP
            record["reasons"].append("only subtype-4 fields; representation-independent")
        return record

    if declared_rep == UuidRepresentation.STANDARD and OLD_UUID_SUBTYPE in subtypes:
        record["status"] = STATUS_LABEL_MISMATCH
        record["reasons"].append(
            "declared STANDARD but sample contains subtype-3 bytes; "
            "STANDARD clients never write subtype 3"
        )
        return record

    if (
        declared_rep
        in (
            UuidRepresentation.PYTHON_LEGACY,
            UuidRepresentation.JAVA_LEGACY,
            UuidRepresentation.CSHARP_LEGACY,
        )
        and OLD_UUID_SUBTYPE not in subtypes
    ):
        record["status"] = STATUS_LABEL_MISMATCH
        record["reasons"].append(
            f"declared {source_name} but sample contains only subtype-4 bytes; "
            "legacy clients write subtype 3"
        )
        return record

    if subtypes == {UUID_SUBTYPE}:
        record["status"] = STATUS_KEEP
        record["reasons"].append("already STANDARD subtype 4")
        return record

    # Legacy subtype 3 with a known source: plan the rewrite and verify it.
    decoded = BSON(raw).decode(codec_options=_codec(declared_rep))
    rewritten = BSON.encode(decoded, codec_options=_codec(UuidRepresentation.STANDARD))
    record["rewritten_bson_hex"] = rewritten.hex()
    new_values = dict(
        walk_binaries(BSON(rewritten).decode(codec_options=_codec(UuidRepresentation.STANDARD)))
    )
    verified = True
    for field in record["uuid_fields"]:
        new_value = new_values[field["path"]]
        field["rewrite"] = {
            "from_subtype": field["subtype"],
            "to_subtype": UUID_SUBTYPE,
            "from_hex": field["stored_hex"],
            "to_hex": _stored_bytes(new_value).hex(),
        }
        # Verify: the rewritten bytes must decode back to the same UUID.
        if _as_uuid_str(new_value, UuidRepresentation.STANDARD) != field["canonical_uuid"]:
            verified = False
            record["reasons"].append(
                f"rewrite verification failed at {field['path']}: "
                f"expected {field['canonical_uuid']}, got "
                f"{_as_uuid_str(new_value, UuidRepresentation.STANDARD)}"
            )
    record["rewrite_verified"] = verified
    record["status"] = STATUS_REWRITE if verified else STATUS_ERROR
    if verified:
        record["reasons"].append(
            f"declared {source_name}; rewrite to STANDARD verified by re-decoding"
        )
        record["auto_convertible"] = True
    return record


def _ejson_options(mode: int, rep: int) -> JSONOptions:
    return JSONOptions(json_mode=mode, uuid_representation=rep)


def _uuid_snapshot(raw: bytes, rep: int) -> dict[str, str]:
    """Decode *raw* under *rep* and map each UUID field path to its UUID string."""
    doc = BSON(raw).decode(codec_options=_codec(rep))
    snapshot = {}
    for path, value in walk_binaries(doc):
        decoded = _as_uuid_str(value, rep)
        if decoded is not None:
            snapshot[path] = decoded
    return snapshot


def _subtype_map(raw: bytes) -> dict[str, int]:
    doc = BSON(raw).decode(codec_options=_codec(UuidRepresentation.UNSPECIFIED))
    return {path: value.subtype for path, value in walk_binaries(doc) if isinstance(value, Binary)}


def _run_ejson_probes(record: dict[str, Any], raw: bytes) -> list[dict[str, Any]]:
    """Probe Extended JSON export/import round trips for one record.

    Each probe exports the sample with one set of JSON options and re-imports
    it with another, then compares the resulting BSON bytes and decoded UUIDs
    against the original sample.  This surfaces the classic hazards:

    * re-importing subtype-3 ``$binary`` with STANDARD options silently
      assumes PYTHON_LEGACY byte order (see ``bson.json_util``), corrupting
      JAVA_LEGACY / CSHARP_LEGACY identities;
    * re-importing a ``$binary``/``$uuid`` payload with a legacy
      representation flips subtype 4 back to subtype 3.
    """
    source_name = record["declared_source"]
    declared_rep = REPRESENTATIONS[source_name] if source_name else None
    export_rep = declared_rep if declared_rep is not None else UuidRepresentation.UNSPECIFIED
    doc = BSON(raw).decode(codec_options=_codec(export_rep))

    probes = [
        # (name, export mode, export rep, import mode, import rep)
        (
            "canonical_unspecified_roundtrip",
            JSONMode.CANONICAL,
            UuidRepresentation.UNSPECIFIED,
            JSONMode.CANONICAL,
            UuidRepresentation.UNSPECIFIED,
        ),
        (
            "relaxed_standard_roundtrip",
            JSONMode.RELAXED,
            UuidRepresentation.STANDARD,
            JSONMode.RELAXED,
            UuidRepresentation.STANDARD,
        ),
        (
            "relaxed_standard_export_legacy_import",
            JSONMode.RELAXED,
            UuidRepresentation.STANDARD,
            JSONMode.RELAXED,
            UuidRepresentation.PYTHON_LEGACY,
        ),
    ]
    if declared_rep is not None and declared_rep != UuidRepresentation.STANDARD:
        probes.append(
            (
                "legacy_export_standard_import",
                JSONMode.RELAXED,
                declared_rep,
                JSONMode.RELAXED,
                UuidRepresentation.STANDARD,
            )
        )

    before_subtypes = _subtype_map(raw)
    raw_doc = BSON(raw).decode(codec_options=_codec(UuidRepresentation.UNSPECIFIED))
    results = []
    for name, out_mode, out_rep, in_mode, in_rep in probes:
        probe: dict[str, Any] = {"name": name}
        try:
            export_doc = raw_doc if out_rep == UuidRepresentation.UNSPECIFIED else doc
            exported = ejson_dumps(export_doc, json_options=_ejson_options(out_mode, out_rep))
            reimported_doc = ejson_loads(exported, json_options=_ejson_options(in_mode, in_rep))
            # Re-encode with the import representation: this is what a naive
            # EJSON pipeline would store.
            encode_rep = (
                in_rep if in_rep != UuidRepresentation.UNSPECIFIED else UuidRepresentation.STANDARD
            )
            reencoded = BSON.encode(reimported_doc, codec_options=_codec(encode_rep))
            probe["exported_json"] = exported
            probe["reencoded_hex"] = reencoded.hex()
            probe["bytes_preserved"] = reencoded == raw
            compare_rep = (
                export_rep
                if export_rep != UuidRepresentation.UNSPECIFIED
                else UuidRepresentation.STANDARD
            )
            probe["uuids_preserved"] = _uuid_snapshot(raw, compare_rep) == _uuid_snapshot(
                reencoded, compare_rep
            )
            after_subtypes = _subtype_map(reencoded)
            probe["subtype_changes"] = {
                path: f"{before_subtypes[path]}->{after_subtypes[path]}"
                for path in before_subtypes
                if path in after_subtypes and before_subtypes[path] != after_subtypes[path]
            }
            probe["hazard"] = not probe["bytes_preserved"] or not probe["uuids_preserved"]
        except Exception as exc:
            probe["error"] = str(exc)
            probe["hazard"] = True
        results.append(probe)
    return results


def _detect_collisions(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Find identities that collide once everything is migrated to STANDARD.

    Two stored byte forms that decode to the same UUID become byte-identical
    after migration: unique indexes would see duplicates and distinct rows
    would suddenly refer to one identity.
    """
    by_uuid: dict[str, list[dict[str, str]]] = {}
    for record in records:
        for field in record["uuid_fields"]:
            canonical = field.get("canonical_uuid")
            if canonical is None:
                continue
            by_uuid.setdefault(canonical, []).append(
                {
                    "record_id": record["record_id"],
                    "path": field["path"],
                    "stored_hex": field["stored_hex"],
                    "subtype": str(field["subtype"]),
                }
            )
    collisions = []
    for canonical, members in sorted(by_uuid.items()):
        distinct_forms = {(m["subtype"], m["stored_hex"]) for m in members}
        distinct_records = {m["record_id"] for m in members}
        if len(distinct_forms) > 1 or len(distinct_records) > 1:
            collisions.append(
                {
                    "canonical_uuid": canonical,
                    "members": members,
                    "distinct_stored_forms": len(distinct_forms),
                    "post_migration": (
                        "all forms migrate to identical subtype-4 bytes; "
                        "unique indexes would report duplicates and separate "
                        "rows would merge into one identity"
                    ),
                }
            )
    return collisions


def check_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    """Run all checks over a manifest and return the full report."""
    records: list[dict[str, Any]] = []
    for sample in manifest.get("samples", []):
        raw = bytes.fromhex(sample["bson_hex"])
        record = analyze_record(
            record_id=sample["id"],
            kind=sample.get("kind", "unknown"),
            source_name=sample.get("source_representation"),
            raw=raw,
        )
        if record["uuid_fields"] and record["status"] != STATUS_ERROR:
            record["ejson_probes"] = _run_ejson_probes(record, raw)
        records.append(record)

    collisions = _detect_collisions(records)
    collided_ids = {m["record_id"] for c in collisions for m in c["members"]}
    for record in records:
        if record["record_id"] in collided_ids:
            record["auto_convertible"] = False
            record["reasons"].append(
                "identity collision: another sample holds the same canonical "
                "UUID in different bytes; automatic conversion would create "
                "duplicates"
            )

    status_counts: dict[str, int] = {}
    for record in records:
        status_counts[record["status"]] = status_counts.get(record["status"], 0) + 1
    unsafe = [
        r["record_id"]
        for r in records
        if r["uuid_fields"] and not r["auto_convertible"] and r["status"] != STATUS_KEEP
    ]

    return {
        "tool": "uuid-migration-check",
        "pymongo_version": pymongo.version,
        "summary": {
            "total_records": len(records),
            "status_counts": status_counts,
            "collision_groups": len(collisions),
            "ejson_hazard_records": [
                r["record_id"] for r in records if any(p.get("hazard") for p in r["ejson_probes"])
            ],
            "unsafe_records": unsafe,
        },
        "records": records,
        "collisions": collisions,
    }


def build_plan(report: dict[str, Any]) -> dict[str, Any]:
    """Derive a migration plan from a check report.

    The plan is data only -- executing it (against a backup, with the
    application offline) is a separate, deliberate step.
    """
    steps = []
    for record in report["records"]:
        if not record["uuid_fields"]:
            action = "skip"
        elif record["auto_convertible"]:
            action = "rewrite"
        elif record["status"] == STATUS_KEEP:
            action = "keep"
        else:
            action = "manual_review"
        step: dict[str, Any] = {
            "record_id": record["record_id"],
            "action": action,
            "status": record["status"],
            "reasons": record["reasons"],
        }
        if action == "rewrite":
            step["changes"] = [
                {
                    "path": f["path"],
                    "canonical_uuid": f["canonical_uuid"],
                    "from_subtype": f["subtype"],
                    "to_subtype": UUID_SUBTYPE,
                    "from_hex": f["stored_hex"],
                    "to_hex": f["rewrite"]["to_hex"],
                }
                for f in record["uuid_fields"]
            ]
            step["rewritten_bson_hex"] = record["rewritten_bson_hex"]
        steps.append(step)
    return {
        "preconditions": [
            "run against a backup or a maintenance window, never live traffic",
            "deploy application code reading STANDARD before rewriting data",
            "resolve every manual_review record before running rewrites",
        ],
        "steps": steps,
    }
