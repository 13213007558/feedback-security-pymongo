"""Generate a labelled BSON sample set reproducing a mixed-representation fleet.

The scenario: an account service migrated its clients to STANDARD UUID
representation, but historical rows were written by PYTHON_LEGACY,
JAVA_LEGACY and CSHARP_LEGACY clients, some rows have no provenance at all,
and one identity exists in two different byte forms.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from bson import BSON, CodecOptions
from bson.binary import UuidRepresentation

_U1 = UUID("00112233-4455-6677-8899-aabbccddeeff")
_U2 = UUID("6f1a2b3c-4d5e-6f70-8192-a3b4c5d6e7f8")
_U3 = UUID("9e8d7c6b-5a49-3827-1605-f4e3d2c1b0a9")
_U4 = UUID("ffffffff-1111-2222-3333-444444444444")
_U5 = UUID("0a0b0c0d-0e0f-1011-1213-141516171819")
_U6 = UUID("deadbeef-1234-5678-9abc-def012345678")
_U7 = UUID("c001d00d-1234-4abc-8def-0a1b2c3d4e5f")
_U8 = UUID("55555555-6666-4777-8888-999999999999")


def _encode(doc: dict[str, Any], rep: int) -> str:
    return BSON.encode(doc, codec_options=CodecOptions(uuid_representation=rep)).hex()


def _sample(
    record_id: str,
    kind: str,
    doc: dict[str, Any],
    written_as: int,
    source: str | None,
) -> dict[str, Any]:
    return {
        "id": record_id,
        "kind": kind,
        "source_representation": source,
        "bson_hex": _encode(doc, written_as),
    }


def generate_manifest() -> dict[str, Any]:
    """Build the demo manifest: labelled BSON samples as hex strings."""
    py = UuidRepresentation.PYTHON_LEGACY
    java = UuidRepresentation.JAVA_LEGACY
    cs = UuidRepresentation.CSHARP_LEGACY
    std = UuidRepresentation.STANDARD
    samples = [
        _sample("acct-001", "account", {"_id": "acct-001", "user_id": _U1}, py, "PYTHON_LEGACY"),
        _sample("acct-002", "account", {"_id": "acct-002", "user_id": _U2}, java, "JAVA_LEGACY"),
        _sample("acct-003", "account", {"_id": "acct-003", "user_id": _U3}, cs, "CSHARP_LEGACY"),
        _sample("acct-004", "account", {"_id": "acct-004", "user_id": _U4}, std, "STANDARD"),
        # Written by a Java service, but the provenance label was lost.
        _sample("acct-005", "account", {"_id": "acct-005", "user_id": _U5}, java, None),
        # The same identity stored twice, in two different representations.
        _sample("acct-006", "account", {"_id": "acct-006", "user_id": _U6}, std, "STANDARD"),
        _sample("acct-007", "account", {"_id": "acct-007", "user_id": _U6}, py, "PYTHON_LEGACY"),
        # Audit rows: the actor UUID reads differently per representation.
        _sample(
            "audit-001",
            "audit",
            {"_id": "audit-001", "actor": _U7, "action": "login"},
            cs,
            "CSHARP_LEGACY",
        ),
        # Audit row whose producing service is unknown.
        _sample(
            "audit-002",
            "audit",
            {"_id": "audit-002", "actor": _U8, "action": "password_reset"},
            py,
            None,
        ),
    ]
    return {
        "description": (
            "Mixed-representation UUID samples. source_representation declares "
            "which client representation wrote the bytes; null means unknown."
        ),
        "samples": samples,
    }
