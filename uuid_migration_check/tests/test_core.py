"""Offline tests for the UUID migration checker (no MongoDB server needed)."""

from uuid import UUID

import pytest

from bson import BSON, CodecOptions
from bson.binary import UuidRepresentation
from uuid_migration_check.core import (
    STATUS_AMBIGUOUS,
    STATUS_KEEP,
    STATUS_LABEL_MISMATCH,
    STATUS_REWRITE,
    analyze_record,
    build_plan,
    check_manifest,
    decode_matrix,
)
from uuid_migration_check.samples import generate_manifest

_U = UUID("00112233-4455-6677-8899-aabbccddeeff")

pytestmark = pytest.mark.default


def _encode(doc, rep):
    return BSON.encode(doc, codec_options=CodecOptions(uuid_representation=rep))


def _stored(doc, rep, key):
    raw = _encode(doc, rep)
    decoded = BSON(raw).decode(
        codec_options=CodecOptions(uuid_representation=UuidRepresentation.UNSPECIFIED)
    )
    return decoded[key]


def test_decode_matrix_shows_three_legacy_interpretations():
    stored = _stored({"user_id": _U}, UuidRepresentation.JAVA_LEGACY, "user_id")
    matrix = decode_matrix(stored)
    assert matrix["JAVA_LEGACY"] == str(_U)
    assert matrix["PYTHON_LEGACY"] != str(_U)
    assert matrix["CSHARP_LEGACY"] != str(_U)
    assert matrix["STANDARD"] is None  # subtype 3 stays Binary under STANDARD


def test_ambiguous_subtype3_is_never_rewritten():
    raw = _encode({"user_id": _U}, UuidRepresentation.JAVA_LEGACY)
    record = analyze_record("r1", "account", None, raw)
    assert record["status"] == STATUS_AMBIGUOUS
    assert record["rewritten_bson_hex"] is None
    assert not record["auto_convertible"]


def test_known_source_rewrite_roundtrips_to_same_uuid():
    for name in ("PYTHON_LEGACY", "JAVA_LEGACY", "CSHARP_LEGACY"):
        rep = getattr(UuidRepresentation, name)
        raw = _encode({"user_id": _U}, rep)
        record = analyze_record("r1", "account", name, raw)
        assert record["status"] == STATUS_REWRITE, name
        assert record["rewrite_verified"]
        assert record["auto_convertible"]
        field = record["uuid_fields"][0]
        assert field["canonical_uuid"] == str(_U)
        assert field["rewrite"]["from_subtype"] == 3
        assert field["rewrite"]["to_subtype"] == 4
        # Rewritten bytes must be the STANDARD encoding of the same UUID.
        assert (
            record["rewritten_bson_hex"]
            == _encode({"user_id": _U}, UuidRepresentation.STANDARD).hex()
        )


def test_query_bytes_match_only_the_writing_representation():
    raw = _encode({"user_id": _U}, UuidRepresentation.CSHARP_LEGACY)
    record = analyze_record("r1", "account", "CSHARP_LEGACY", raw)
    query = record["uuid_fields"][0]["query_bytes"]
    matching = [name for name, q in query.items() if q["matches_stored_bytes"]]
    assert matching == ["CSHARP_LEGACY"]


def test_standard_sample_is_kept():
    raw = _encode({"user_id": _U}, UuidRepresentation.STANDARD)
    record = analyze_record("r1", "account", "STANDARD", raw)
    assert record["status"] == STATUS_KEEP


def test_label_mismatch_standard_declared_on_subtype3():
    raw = _encode({"user_id": _U}, UuidRepresentation.PYTHON_LEGACY)
    record = analyze_record("r1", "account", "STANDARD", raw)
    assert record["status"] == STATUS_LABEL_MISMATCH
    assert not record["auto_convertible"]


def test_collision_blocks_auto_conversion():
    manifest = generate_manifest()
    report = check_manifest(manifest)
    assert len(report["collisions"]) == 1
    collision = report["collisions"][0]
    assert {m["record_id"] for m in collision["members"]} == {"acct-006", "acct-007"}
    records = {r["record_id"]: r for r in report["records"]}
    assert not records["acct-007"]["auto_convertible"]
    plan = build_plan(report)
    steps = {s["record_id"]: s for s in plan["steps"]}
    assert steps["acct-007"]["action"] == "manual_review"
    assert steps["acct-001"]["action"] == "rewrite"
    assert steps["acct-005"]["action"] == "manual_review"
    assert "acct-005" in report["summary"]["unsafe_records"]
    assert "acct-004" not in report["summary"]["unsafe_records"]


def test_ejson_standard_import_of_unknown_subtype3_corrupts_identity():
    manifest = generate_manifest()
    report = check_manifest(manifest)
    records = {r["record_id"]: r for r in report["records"]}
    probes = {p["name"]: p for p in records["acct-005"]["ejson_probes"]}
    probe = probes["relaxed_standard_roundtrip"]
    assert probe["hazard"]
    assert not probe["uuids_preserved"]
    assert probe["subtype_changes"] == {"user_id": "3->4"}
    # The lossless baseline must stay clean.
    assert not probes["canonical_unspecified_roundtrip"]["hazard"]


def test_ejson_legacy_import_flips_subtype4_back_to_3():
    manifest = generate_manifest()
    report = check_manifest(manifest)
    records = {r["record_id"]: r for r in report["records"]}
    probes = {p["name"]: p for p in records["acct-004"]["ejson_probes"]}
    probe = probes["relaxed_standard_export_legacy_import"]
    assert probe["hazard"]
    assert probe["subtype_changes"] == {"user_id": "4->3"}
