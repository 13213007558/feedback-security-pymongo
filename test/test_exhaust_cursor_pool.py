# Copyright 2026-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Exhaust cursor connection-reuse regression tests.

These tests use an in-process, socketpair-backed MongoDB wire protocol mock
(no listening socket required). They assert that an exhaust cursor whose
stream is not fully consumed never leaves its pinned connection in a state
where another operation can read a leftover moreToCome response.
"""

from __future__ import annotations

import socket
import struct
import threading
from typing import Any

import bson

from pymongo.cursor_shared import CursorType
from pymongo.errors import OperationFailure, ProtocolError
from pymongo.network_layer import NetworkingInterface
from pymongo import monitoring

import pymongo.synchronous.pool as pool_mod
from test import UnitTest, unittest

_IS_SYNC = True

OP_MSG = 2013
MORE_TO_COME = 1 << 1
CURSOR_ID = 4242


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("mock server saw EOF")
        data += chunk
    return data


def _read_request(sock: socket.socket) -> dict[str, Any]:
    length, req_id, _response_to, _op_code = struct.unpack("<iiii", _recv_exact(sock, 16))
    body = _recv_exact(sock, length - 16)
    flags = struct.unpack("<I", body[:4])[0]
    return {"req_id": req_id, "flags": flags, "doc": bson.decode(body[5:])}


def _op_msg_reply(req_id: int, doc: dict, flags: int = 0) -> bytes:
    payload = struct.pack("<I", flags) + b"\x00" + bson.BSON.encode(doc)
    header = struct.pack("<iiii", 16 + len(payload), 1, req_id, OP_MSG)
    return header + payload


def _hello() -> dict:
    return {
        "ok": 1,
        "ismaster": True,
        "minWireVersion": 0,
        "maxWireVersion": 25,
        "connectionId": 1,
        "topologyVersion": {"processId": bson.ObjectId(), "counter": 0},
    }


def _batch(cursor_id: int, key: str, docs: list, final: bool = False) -> dict:
    return {"ok": 1, "cursor": {"id": 0 if final else cursor_id, "ns": "db.coll", key: docs}}


class _MockServer:
    """A minimal OP_MSG mock for one driver connection."""

    def __init__(self, handler):
        self.handler = handler
        self.server_sock, self.client_sock = socket.socketpair()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        try:
            while True:
                request = _read_request(self.server_sock)
                name = next(iter(request["doc"]))
                if name in ("hello", "ismaster"):
                    self.server_sock.sendall(_op_msg_reply(request["req_id"], _hello()))
                    continue
                for action in self.handler(request):
                    kind = action[0]
                    if kind == "reply":
                        self.server_sock.sendall(
                            _op_msg_reply(request["req_id"], action[1], action[2])
                        )
                    elif kind == "raw":
                        self.server_sock.sendall(action[1])
        except OSError:
            # The driver closed the socket; that is an expected outcome.
            pass

    def close(self) -> None:
        for sock in (self.server_sock, self.client_sock):
            try:
                sock.close()
            except OSError:
                pass


class _PoolListener(monitoring.ConnectionPoolListener):
    def __init__(self) -> None:
        self.closed_ids: set[int] = set()

    def connection_closed(self, event) -> None:  # type: ignore[override]
        self.closed_ids.add(event.connection_id)


class TestExhaustCursorPoolReuse(UnitTest):
    def setUp(self) -> None:
        self.mocks: list[_MockServer] = []
        self._original_factory = pool_mod._configured_socket_interface

        def factory(address, options, ssl_session_cache=None):
            mock = _MockServer(self.handler)
            self.mocks.append(mock)
            mock.client_sock.settimeout(options.socket_timeout)
            return NetworkingInterface(mock.client_sock)

        pool_mod._configured_socket_interface = factory

    def tearDown(self) -> None:
        pool_mod._configured_socket_interface = self._original_factory
        for mock in self.mocks:
            mock.close()

    def make_client(self, listener: _PoolListener):
        from pymongo.synchronous.mongo_client import MongoClient

        return MongoClient(
            "mongodb://localhost:27017/?directConnection=true"
            "&serverSelectionTimeoutMS=5000&socketTimeoutMS=5000",
            event_listeners=[listener],
        )

    @staticmethod
    def pool_of(client):
        server = next(iter(client._topology._servers.values()))
        return server.pool

    def test_mid_stream_error_closes_connection(self) -> None:
        def handler(request: dict):
            if next(iter(request["doc"])) == "find":
                return [
                    ("reply",
                     {"ok": 1, "cursor": {"id": CURSOR_ID, "ns": "db.coll",
                                           "firstBatch": [{"v": 1}]}},
                     MORE_TO_COME),
                    ("reply", _batch(CURSOR_ID, "nextBatch", [{"v": 2}]), MORE_TO_COME),
                    ("reply", {"ok": 0, "code": 43, "errmsg": "boom"}, MORE_TO_COME),
                    ("reply", _batch(CURSOR_ID, "nextBatch", [{"v": 4}], final=True), 0),
                ]
            return [("reply", {"ok": 1}, 0)]

        self.handler = handler
        listener = _PoolListener()
        client = self.make_client(listener)
        try:
            cursor = client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)
            cursor.next()
            connection = cursor._sock_mgr.conn
            connection_id = connection.id
            with self.assertRaises(OperationFailure):
                cursor.to_list()
            # The failure must mark the connection and the pool must close it
            # rather than hand its buffered bytes to another operation.
            self.assertIn(connection_id, listener.closed_ids)
            self.assertTrue(connection.closed)
            self.assertFalse(any(c.id == connection_id for c in self.pool_of(client).conns))
            # A subsequent, unrelated request must get its own fresh response.
            self.assertEqual(client.admin.command("ping"), {"ok": 1})
        finally:
            client.close()

    def test_early_close_with_unread_batches_destroys_socket(self) -> None:
        def handler(request: dict):
            if next(iter(request["doc"])) == "find":
                return [
                    ("reply",
                     {"ok": 1, "cursor": {"id": CURSOR_ID, "ns": "db.coll",
                                           "firstBatch": [{"v": 1}]}},
                     MORE_TO_COME),
                    ("reply", _batch(CURSOR_ID, "nextBatch", [{"v": 2}]), MORE_TO_COME),
                    ("reply", _batch(CURSOR_ID, "nextBatch", [{"v": 3}], final=True), 0),
                ]
            return [("reply", {"ok": 1}, 0)]

        self.handler = handler
        listener = _PoolListener()
        client = self.make_client(listener)
        try:
            cursor = client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)
            cursor.next()
            connection = cursor._sock_mgr.conn
            connection_id = connection.id
            cursor.close()
            self.assertIn(connection_id, listener.closed_ids)
            self.assertTrue(connection.closed)
            self.assertEqual(client.admin.command("ping"), {"ok": 1})
        finally:
            client.close()

    def test_fully_consumed_stream_returns_connection(self) -> None:
        def handler(request: dict):
            if next(iter(request["doc"])) == "find":
                return [
                    ("reply",
                     {"ok": 1, "cursor": {"id": CURSOR_ID, "ns": "db.coll",
                                           "firstBatch": [{"v": 1}]}},
                     MORE_TO_COME),
                    ("reply", _batch(CURSOR_ID, "nextBatch", [{"v": 2}]), MORE_TO_COME),
                    ("reply", _batch(CURSOR_ID, "nextBatch", [{"v": 3}], final=True), 0),
                ]
            return [("reply", {"ok": 1}, 0)]

        self.handler = handler
        listener = _PoolListener()
        client = self.make_client(listener)
        try:
            values = [doc["v"] for doc in
                      client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)]
            self.assertEqual(values, [1, 2, 3])
            pool = self.pool_of(client)
            self.assertTrue(pool.conns)
            self.assertEqual(client.admin.command("ping"), {"ok": 1})
        finally:
            client.close()

    def test_stray_continuation_on_non_exhaust_request_is_rejected(self) -> None:
        def handler(request: dict):
            if next(iter(request["doc"])) == "ping":
                good = _op_msg_reply(request["req_id"], {"ok": 1}, 0)
                stray = _op_msg_reply(request["req_id"] + 9999, {"ok": 1, "stray": True}, 0)
                return [("raw", good + stray)]
            return [("reply", {"ok": 1}, 0)]

        self.handler = handler
        listener = _PoolListener()
        client = self.make_client(listener)
        try:
            client.admin.command("ping")
            pool = self.pool_of(client)
            connection_id = next(iter(pool.conns)).id
            with self.assertRaises((ProtocolError, ConnectionError)):
                client.admin.command("ping")
            self.assertIn(connection_id, listener.closed_ids)
            # A fresh connection serves the retry correctly.
            self.assertEqual(client.admin.command("ping"), {"ok": 1})
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
