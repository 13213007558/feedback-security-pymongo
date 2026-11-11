# Copyright 2026-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Isolation tests for the interaction of speculative authentication and
the per-credential SCRAM derived-material cache.

These tests never open a socket and never use real credentials. An in-memory
SCRAM server double records every command the driver sends and keeps a
transcript of the complete SCRAM conversation, so assertions can verify both
the wire-level exchange and the state of the driver-side cache.

The failure mode these tests are built to catch is cache cross-talk: each
``MongoCredential`` owns its own ``_Cache`` instance, and the SCRAM proof sent
in ``saslContinue`` must always be derived from that credential's password,
authSource and the server's current salt and iteration count. A regression
that shares or reuses derived material across credentials (for example because
``_Cache`` instances compare and hash identically) passes when each credential
is exercised in isolation but produces a wrong proof once the authentications
are interleaved in a single process.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import sys
from base64 import standard_b64decode, standard_b64encode
from types import SimpleNamespace

sys.path[0:0] = [""]

from bson.binary import Binary
from pymongo.synchronous import auth as auth_module
from pymongo.synchronous.pool import Connection
from pymongo.auth_shared import (
    MongoCredential,
    _AuthContext,
    _Cache,
    _parse_scram_response,
)
from pymongo.errors import OperationFailure
from pymongo.hello import Hello
from pymongo.saslprep import saslprep
from test import UnitTest, unittest

_IS_SYNC = True

ADDRESS = ("localhost", 27017)
USERNAME = "alice"
# Speculative SCRAM always negotiates SCRAM-SHA-256 (including for DEFAULT).
MECHANISM = "SCRAM-SHA-256"
DIGESTMOD = hashlib.sha256


def _hmac(key: bytes, message: bytes) -> bytes:
    return hmac.HMAC(key, message, DIGESTMOD).digest()


def _scram_credentials(source: str, password: str) -> MongoCredential:
    """Build SCRAM-SHA-256 credentials the same way the URI layer does."""
    return MongoCredential(
        MECHANISM, source, USERNAME, password, None, _Cache()
    )


class ScramServer:
    """Minimal, stateful in-memory SCRAM-SHA-256 server.

    The server holds one user record per (authSource, username). Each record
    carries its own password, salt and iteration count, which a test can
    mutate between handshakes to force cache invalidation. A single instance
    records the commands it received and a per-connection transcript of the
    raw SCRAM messages.
    """

    def __init__(self) -> None:
        # Keyed by authSource so the same username can exist in several
        # databases with different passwords and SCRAM parameters.
        self.users: dict[str, dict[str, object]] = {}
        self.commands: list[dict] = []
        self.transcripts: list[list[tuple[str, bytes]]] = []
        # Maps conversationId -> authSource so interleaved conversations are
        # always validated against the user record they started with.
        self.conversation_sources: dict[int, str] = {}
        # Maps conversationId -> its ordered SCRAM transcript.
        self.conversation_transcripts: dict[int, list] = {}
        self._conversation_id = 0
        # How the server treats the speculative saslStart embedded in hello.
        # "accept" -> complete the first SCRAM round in the hello response.
        # "ignore" -> omit speculativeAuthenticate from the hello response.
        # "mismatch" -> return a first-round answer that does not belong to
        # the client's conversation (wrong nonce).
        self.speculative_mode = "accept"
        self.mechanisms = ["SCRAM-SHA-1", "SCRAM-SHA-256"]
        # When True the final saslContinue answer carries a bad server
        # signature, forcing the driver to reject the exchange.
        self.fail_server_signature = False

    def add_user(
        self,
        source: str,
        password: str,
        salt: bytes | None = None,
        iterations: int = 4096,
    ) -> None:
        if salt is None:
            salt = standard_b64encode(os.urandom(16))
        self.users[source] = {
            "password": password,
            "salt": salt,
            "iterations": iterations,
        }

    def set_parameters(
        self,
        source: str,
        *,
        salt: bytes | None = None,
        iterations: int | None = None,
    ) -> None:
        record = self.users[source]
        if salt is not None:
            record["salt"] = salt
        if iterations is not None:
            record["iterations"] = iterations

    def _keys(self, source: str) -> tuple[bytes, bytes, bytes, int, bytes]:
        record = self.users[source]
        password = record["password"]
        salt = record["salt"]
        iterations = record["iterations"]
        data = saslprep(password).encode("utf-8")
        salted_password = hashlib.pbkdf2_hmac(
            "sha256", data, standard_b64decode(salt), iterations
        )
        client_key = _hmac(salted_password, b"Client Key")
        server_key = _hmac(salted_password, b"Server Key")
        return client_key, server_key, salt, iterations, salted_password

    def _open_transcript(
        self, conversation_id: int, first_entry: tuple[str, bytes]
    ) -> list:
        transcript = [first_entry]
        self.transcripts.append(transcript)
        self.conversation_transcripts[conversation_id] = transcript
        return transcript

    def _first_round(
        self,
        source: str,
        transcript: list,
        client_first_bare: bytes,
        conversation_id: int,
    ) -> dict:
        _, _, salt, iterations, _ = self._keys(source)
        client_nonce = _parse_scram_response(client_first_bare)[b"r"]
        full_nonce = client_nonce + standard_b64encode(os.urandom(16))
        server_first = b"".join(
            (
                b"r=",
                full_nonce,
                b",s=",
                salt,
                b",i=",
                str(iterations).encode("ascii"),
            )
        )
        transcript.append(("server-first", server_first))
        return {
            "conversationId": conversation_id,
            "done": False,
            "payload": Binary(server_first),
        }

    def _mismatched_round(
        self, transcript: list, conversation_id: int
    ) -> dict:
        # Answer with a nonce the client never sent, so the driver's
        # rnonce.startswith(nonce) check must fail.
        foreign_nonce = standard_b64encode(b"not-the-client-nonce-00000000")
        salt = standard_b64encode(os.urandom(16))
        server_first = b"r=" + foreign_nonce + b",s=" + salt + b",i=4096"
        transcript.append(("server-first-mismatch", server_first))
        return {
            "conversationId": conversation_id,
            "done": False,
            "payload": Binary(server_first),
        }

    def handle_hello(self, cmd: dict) -> dict:
        reply: dict = {
            "ok": 1,
            "isWritablePrimary": True,
            "maxWireVersion": 25,
            "connectionId": 1,
        }
        if "saslSupportedMechs" in cmd:
            reply["saslSupportedMechs"] = self.mechanisms
        speculative = cmd.get("speculativeAuthenticate")
        if speculative is None:
            return reply
        # Reserve the conversation id even when the speculative attempt is
        # declined, so the fallback saslStart gets a distinct id.
        self._conversation_id += 1
        conversation_id = self._conversation_id
        if self.speculative_mode == "ignore":
            # The server declined the speculative attempt; no field is set.
            return reply
        source = speculative["db"]
        self.conversation_sources[conversation_id] = source
        transcript = self._open_transcript(
            conversation_id,
            ("client-first-speculative", bytes(speculative["payload"])),
        )
        client_first_bare = bytes(speculative["payload"])[3:]
        if self.speculative_mode == "mismatch":
            reply["speculativeAuthenticate"] = self._mismatched_round(
                transcript, conversation_id
            )
        else:
            reply["speculativeAuthenticate"] = self._first_round(
                source, transcript, client_first_bare, conversation_id
            )
        return reply

    def handle_sasl_start(self, source: str, cmd: dict) -> dict:
        self._conversation_id += 1
        conversation_id = self._conversation_id
        self.conversation_sources[conversation_id] = source
        transcript = self._open_transcript(
            conversation_id, ("client-first", bytes(cmd["payload"]))
        )
        return self._first_round(
            source, transcript, bytes(cmd["payload"])[3:], conversation_id
        )

    def handle_sasl_continue(self, cmd: dict) -> dict:
        conversation_id = cmd["conversationId"]
        source = self.conversation_sources[conversation_id]
        transcript = self.conversation_transcripts[conversation_id]
        client_final = bytes(cmd["payload"])
        transcript.append(("client-final", client_final))
        parsed_client = _parse_scram_response(client_final)
        client_proof = standard_b64decode(parsed_client[b"p"])
        rnonce = parsed_client[b"r"]

        client_key, server_key, _salt, _iterations, _ = self._keys(source)
        stored_key = DIGESTMOD(client_key).digest()
        # Rebuild the auth message from the recorded transcript. Both
        # speculative and standalone first messages carry the "n,," GS2
        # header, which is not part of the SCRAM AuthMessage.
        client_first_bare = transcript[0][1]
        if client_first_bare.startswith(b"n,,"):
            client_first_bare = client_first_bare[3:]
        server_first = transcript[1][1]
        without_proof = b"c=biws,r=" + rnonce
        auth_message = b",".join((client_first_bare, server_first, without_proof))
        client_signature = _hmac(stored_key, auth_message)
        expected_proof = bytes(a ^ b for a, b in zip(client_key, client_signature))

        if not hmac.compare_digest(client_proof, expected_proof):
            transcript.append(("server-error", b"e=invalid-proof"))
            # A real command reply carries ok: 0 and is converted to
            # OperationFailure by the driver's command response check.
            raise OperationFailure(
                "Authentication failed.",
                18,
                {"ok": 0, "errmsg": "Authentication failed.", "code": 18},
            )

        server_signature = standard_b64encode(_hmac(server_key, auth_message))
        if self.fail_server_signature:
            server_signature = standard_b64encode(b"\x00" * len(server_signature))
        server_final = b"v=" + server_signature
        transcript.append(("server-final", server_final))
        return {
            "conversationId": cmd["conversationId"],
            "done": True,
            "payload": Binary(server_final),
        }

    def command(self, dbname: str, cmd: dict, **kwargs: object) -> dict:
        self.commands.append(dict(cmd))
        if "hello" in cmd or "ismaster" in cmd:
            return self.handle_hello(cmd)
        if "saslStart" in cmd:
            return self.handle_sasl_start(dbname, cmd)
        if "saslContinue" in cmd:
            return self.handle_sasl_continue(cmd)
        raise AssertionError(f"unexpected command: {cmd!r}")


class FakeConnection:
    """Connection double exposing only the surface SCRAM auth touches.

    ``handshake`` runs the real ``Connection._hello`` code path so the
    speculative command is built and parsed by production code; ``authenticate``
    runs the real ``auth.authenticate`` entry point.
    """

    def __init__(self, credentials: MongoCredential, server: ScramServer) -> None:
        self._conn = object.__new__(Connection)
        self.server = server
        self.credentials = credentials
        conn = self._conn
        conn.address = ADDRESS
        conn.auth_ctx = None
        conn.negotiated_mechs = None
        conn.hello_ok = False
        conn.performed_handshake = False
        conn.max_wire_version = 25
        conn.is_writable = False
        conn.max_bson_size = 16777216
        conn.max_message_size = 48000000
        conn.max_write_batch_size = 100000
        conn.supports_sessions = False
        conn.is_repl = False
        conn.is_standalone = True
        conn.is_mongos = False
        conn.compression_settings = None
        conn.compression_context = None
        conn.server_connection_id = None
        conn.service_id = None
        conn.pool_gen = SimpleNamespace(
            get=lambda service_id: SimpleNamespace(
                get_overall=lambda: 0, get=lambda service_id: 0
            )
        )
        conn.generation = 0
        conn.opts = SimpleNamespace(
            _credentials=credentials,
            metadata={},
            compression_settings=None,
            load_balanced=False,
            server_api=None,
            connect_timeout=None,
        )
        conn.command = self.command

    def command(self, dbname: str, cmd: dict, **kwargs: object) -> dict:
        return self.server.command(dbname, cmd, **kwargs)

    def handshake(self) -> Hello:
        return self._conn._hello(None, None)

    @property
    def auth_ctx(self) -> _AuthContext | None:
        return self._conn.auth_ctx

    def authenticate(self) -> None:
        auth_module.authenticate(self.credentials, self._conn)


class SpeculativeCacheIsolationTest(UnitTest):
    def _handshake_and_auth(
        self, credentials: MongoCredential, server: ScramServer
    ) -> FakeConnection:
        conn = FakeConnection(credentials, server)
        conn.handshake()
        conn.authenticate()
        return conn

    def test_distinct_credentials_get_distinct_cache_instances(self):
        # _Cache compares and hashes identically on purpose, so isolation
        # depends entirely on each credential holding its own instance.
        first = _scram_credentials("admin", "pw-a")
        second = _scram_credentials("appdb", "pw-b")
        self.assertEqual(first.cache, second.cache)
        self.assertEqual(hash(first.cache), hash(second.cache))
        self.assertIsNot(first.cache, second.cache)
        self.assertIsNot(first, second)
        # Writing one cache must not be visible through the other.
        first.cache.data = (b"a", b"b", b"s", 4096)
        self.assertIsNone(second.cache.data)

    def test_speculative_accepted_uses_cache_and_correct_proof(self):
        server = ScramServer()
        server.add_user("admin", "secret-pw")
        creds = _scram_credentials("admin", "secret-pw")

        self.assertIsNone(creds.cache.data)
        conn = self._handshake_and_auth(creds, server)

        # The saslStart rode along in the hello command; no standalone
        # saslStart was needed.
        names = [next(iter(command)) for command in server.commands]
        self.assertEqual(names, ["ismaster", "saslContinue"])
        self.assertIsNotNone(conn.auth_ctx)
        self.assertTrue(conn.auth_ctx.speculate_succeeded())
        # A successful exchange populates the cache with four elements.
        self.assertIsNotNone(creds.cache.data)
        client_key, _server_key, salt, iterations = creds.cache.data
        self.assertEqual(salt, server.users["admin"]["salt"])
        self.assertEqual(iterations, server.users["admin"]["iterations"])
        # The server independently validated the client proof, so reaching
        # here proves the proof matched this identity.
        self.assertEqual(server.transcripts[-1][-1][0], "server-final")

        # A second connection with the same credential must reuse the cached
        # material and still produce a valid proof.
        conn_two = self._handshake_and_auth(creds, server)
        self.assertEqual(creds.cache.data[0], client_key)
        self.assertEqual(server.transcripts[-1][-1][0], "server-final")
        self.assertTrue(conn_two.auth_ctx.speculate_succeeded())

    def test_speculative_ignored_falls_back_to_full_sasl_start(self):
        server = ScramServer()
        server.speculative_mode = "ignore"
        server.add_user("admin", "secret-pw")
        creds = _scram_credentials("admin", "secret-pw")

        conn = FakeConnection(creds, server)
        conn.handshake()
        # The hello carried a speculative command but the server did not
        # answer it, so no auth context was retained.
        self.assertIsNone(conn.auth_ctx)
        self.assertIn("speculativeAuthenticate", server.commands[0])
        conn.authenticate()

        names = [next(iter(command)) for command in server.commands]
        self.assertEqual(names, ["ismaster", "saslStart", "saslContinue"])
        self.assertIsNotNone(creds.cache.data)
        self.assertEqual(server.transcripts[-1][-1][0], "server-final")

    def test_speculative_mismatch_is_rejected_then_reauth_succeeds(self):
        server = ScramServer()
        server.speculative_mode = "mismatch"
        server.add_user("admin", "secret-pw")
        creds = _scram_credentials("admin", "secret-pw")

        conn = FakeConnection(creds, server)
        conn.handshake()
        self.assertIsNotNone(conn.auth_ctx)
        self.assertTrue(conn.auth_ctx.speculate_succeeded())
        with self.assertRaises(OperationFailure) as context:
            conn.authenticate()
        self.assertIn("nonce", str(context.exception).lower())

        # The failed speculative continuation must not poison the credential:
        # a fresh connection against a well-behaved server authenticates.
        server.speculative_mode = "accept"
        clean = self._handshake_and_auth(creds, server)
        self.assertIsNotNone(creds.cache.data)
        self.assertTrue(clean.auth_ctx.speculate_succeeded())
        self.assertEqual(server.transcripts[-1][-1][0], "server-final")

    def test_interleaved_auth_sources_keep_derived_material_isolated(self):
        server = ScramServer()
        admin_salt = standard_b64encode(b"admin-salt-0000000000000001")
        app_salt = standard_b64encode(b"appdb-salt-000000000000000002")
        server.add_user("admin", "admin-password", salt=admin_salt, iterations=4096)
        server.add_user("appdb", "app-password", salt=app_salt, iterations=8192)
        admin_creds = _scram_credentials("admin", "admin-password")
        app_creds = _scram_credentials("appdb", "app-password")

        # Warm both caches in an interleaved order: admin starts, app finishes
        # first, then admin completes.
        admin_conn = FakeConnection(admin_creds, server)
        admin_conn.handshake()
        app_conn = self._handshake_and_auth(app_creds, server)
        admin_conn.authenticate()

        self.assertEqual(admin_creds.cache.data[2], admin_salt)
        self.assertEqual(admin_creds.cache.data[3], 4096)
        self.assertEqual(app_creds.cache.data[2], app_salt)
        self.assertEqual(app_creds.cache.data[3], 8192)
        self.assertNotEqual(admin_creds.cache.data[0], app_creds.cache.data[0])
        # Each conversation targeted its own authSource.
        self.assertEqual(server.commands[0]["speculativeAuthenticate"]["db"], "admin")
        self.assertEqual(server.commands[1]["speculativeAuthenticate"]["db"], "appdb")
        self.assertEqual(app_conn.credentials.source, "appdb")

        # Re-interleave after both caches are warm. A shared-cache regression
        # would now emit a proof derived from the wrong credential.
        for _ in range(3):
            first = FakeConnection(admin_creds, server)
            first.handshake()
            second = self._handshake_and_auth(app_creds, server)
            first.authenticate()
            self.assertTrue(first.auth_ctx.speculate_succeeded())
            self.assertTrue(second.auth_ctx.speculate_succeeded())
        self.assertEqual(admin_creds.cache.data[2], admin_salt)
        self.assertEqual(app_creds.cache.data[2], app_salt)

    def test_same_user_and_source_distinct_passwords_are_isolated(self):
        server = ScramServer()
        server.add_user("admin", "first-password")
        first_creds = _scram_credentials("admin", "first-password")
        second_creds = _scram_credentials("admin", "second-password")
        self.assertIsNot(first_creds.cache, second_creds.cache)

        self._handshake_and_auth(first_creds, server)
        first_cached = first_creds.cache.data
        # Rotate the server record to the second password with new SCRAM
        # parameters, then authenticate the second credential.
        new_salt = standard_b64encode(b"rotated-salt-0000000000000003")
        server.add_user("admin", "second-password", salt=new_salt, iterations=10000)
        self._handshake_and_auth(second_creds, server)

        self.assertEqual(first_creds.cache.data, first_cached)
        self.assertEqual(second_creds.cache.data[2], new_salt)
        self.assertEqual(second_creds.cache.data[3], 10000)
        self.assertNotEqual(first_creds.cache.data[0], second_creds.cache.data[0])

        # Interleave fresh handshakes against both credentials. Both caches
        # are warm, so a shared-cache regression would make one side present
        # a proof derived from the other password and fail server validation.
        second_cached = second_creds.cache.data
        for _ in range(3):
            first_conn = FakeConnection(first_creds, server)
            first_conn.handshake()
            second_conn = FakeConnection(second_creds, server)
            second_conn.handshake()
            with self.assertRaises(OperationFailure):
                # The server no longer knows the first password.
                first_conn.authenticate()
            second_conn.authenticate()
        # The surviving credential's derived material is never affected by
        # the other credential's failures.
        self.assertEqual(second_creds.cache.data, second_cached)
        self.assertEqual(second_creds.cache.data[2], new_salt)
        self.assertEqual(second_creds.cache.data[3], 10000)

    def test_salt_and_iteration_changes_invalidate_stale_cache(self):
        server = ScramServer()
        server.add_user("admin", "secret-pw", iterations=4096)
        creds = _scram_credentials("admin", "secret-pw")
        self._handshake_and_auth(creds, server)
        original = creds.cache.data
        self.assertEqual(original[3], 4096)

        # The server rotates the salt but keeps the same password. The cached
        # keys no longer match, so the driver recomputes and the proof works.
        rotated_salt = standard_b64encode(b"rotated-salt-0000000000000004")
        server.set_parameters("admin", salt=rotated_salt)
        self._handshake_and_auth(creds, server)
        self.assertEqual(creds.cache.data[2], rotated_salt)
        self.assertNotEqual(creds.cache.data[0], original[0])

        # Now only the iteration count changes.
        server.set_parameters("admin", iterations=12000)
        self._handshake_and_auth(creds, server)
        self.assertEqual(creds.cache.data[3], 12000)

        # An unsupported iteration count (< 4096) is rejected outright and
        # does not leave a half-populated cache behind.
        server.set_parameters("admin", iterations=2048)
        with self.assertRaises(OperationFailure):
            self._handshake_and_auth(creds, server)
        self.assertEqual(creds.cache.data[3], 12000)

    def test_failed_signature_does_not_pollute_next_legitimate_auth(self):
        server = ScramServer()
        server.add_user("admin", "secret-pw")
        creds = _scram_credentials("admin", "secret-pw")

        server.fail_server_signature = True
        with self.assertRaises(OperationFailure) as context:
            self._handshake_and_auth(creds, server)
        self.assertIn("signature", str(context.exception).lower())
        cached_after_failure = creds.cache.data

        # The next attempt against an honest server succeeds without any
        # residual effect from the bad conversation.
        server.fail_server_signature = False
        conn = self._handshake_and_auth(creds, server)
        self.assertTrue(conn.auth_ctx.speculate_succeeded())
        self.assertEqual(creds.cache.data, cached_after_failure)
        self.assertEqual(server.transcripts[-1][-1][0], "server-final")

    def test_mechanism_removal_does_not_reuse_stale_speculative_state(self):
        server = ScramServer()
        server.add_user("admin", "secret-pw")
        creds = _scram_credentials("admin", "secret-pw")
        self._handshake_and_auth(creds, server)
        self.assertEqual(
            creds.cache.data[2:],
            (server.users["admin"]["salt"], server.users["admin"]["iterations"]),
        )

        # The server stops offering SCRAM-SHA-256. The explicit mechanism on
        # the credential still drives SCRAM-SHA-256, but a changed salt forces
        # fresh key derivation rather than reuse of the old cached material.
        server.mechanisms = ["SCRAM-SHA-1"]
        rotated_salt = standard_b64encode(b"mechanism-salt-0000000000005")
        server.set_parameters("admin", salt=rotated_salt, iterations=4096)
        conn = self._handshake_and_auth(creds, server)
        self.assertEqual(creds.cache.data[2], rotated_salt)
        self.assertEqual(server.transcripts[-1][-1][0], "server-final")
        self.assertTrue(conn.auth_ctx.speculate_succeeded())


if __name__ == "__main__":
    unittest.main()
