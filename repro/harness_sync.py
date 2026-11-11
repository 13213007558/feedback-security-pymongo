"""Synchronous counterpart of harness_async.py.

Verifies the generated pymongo.synchronous driver over blocking sockets:
exhaust early close (A), mid-stream error (D), truncated frame (C), a stray
non-exhaust continuation (E), and preserved normal streaming (F). Cancellation
(B) is async-only; the sync analog is an exception during iteration.
"""
from __future__ import annotations

import sys
import threading
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pymongo.synchronous.pool as sync_pool
from pymongo.network_layer import NetworkingInterface
from pymongo import monitoring
from pymongo.cursor_shared import CursorType
from pymongo.errors import ConnectionFailure, OperationFailure, ProtocolError

from mock_server import MockServer, MORE_TO_COME, cursor_doc, op_msg_reply

CURSOR_ID = 4242


class CMapLog(monitoring.ConnectionPoolListener):
    def __init__(self):
        self.events = []

    def _record(self, name, event):
        self.events.append((name, getattr(event, "connection_id", None), getattr(event, "reason", None)))

    connection_created = lambda self, e: self._record("created", e)
    connection_ready = lambda self, e: self._record("ready", e)
    connection_closed = lambda self, e: self._record("closed", e)
    connection_checked_out = lambda self, e: self._record("checked_out", e)
    connection_checked_in = lambda self, e: self._record("checked_in", e)
    connection_check_out_failed = lambda self, e: self._record("checkout_failed", e)
    connection_check_out_started = lambda self, e: None

    def timeline(self, conn_id):
        return [e for e in self.events if e[1] == conn_id]


MOCKS: list[MockServer] = []
_CURRENT_HANDLER = [lambda mock, req: [("reply", {"ok": 1}, 0)]]


def install_factory():
    def fake_factory(address, options, ssl_session_cache=None):
        mock = MockServer(_CURRENT_HANDLER[0], label=f"conn{len(MOCKS)}")
        MOCKS.append(mock)
        mock.cli_sock.settimeout(options.socket_timeout)
        return NetworkingInterface(mock.cli_sock)

    sync_pool._configured_socket_interface = fake_factory


def set_handler(fn):
    _CURRENT_HANDLER[0] = fn


def exhaust_stream(mock, req, batches, error_on=None, truncate_at=None):
    actions = []
    n = len(batches)
    for i, batch in enumerate(batches):
        last = i == n - 1
        flags = 0 if last else MORE_TO_COME
        if error_on is not None and i == error_on:
            actions.append(("reply", {"ok": 0, "code": 43, "errmsg": "mid-stream failure"}, flags))
        elif truncate_at is not None and i == truncate_at:
            good = op_msg_reply(req["req_id"], cursor_doc(CURSOR_ID, "nextBatch", batch), flags)
            actions.append(("raw", good[: len(good) // 2]))
        else:
            actions.append(("reply", cursor_doc(CURSOR_ID, "nextBatch", batch, final=last), flags))
    return actions


def find_handler(stream_builder):
    def handler(mock, req):
        name = next(iter(req["doc"]))
        if name == "find":
            first = ("reply",
                     {"ok": 1, "cursor": {"id": CURSOR_ID, "ns": "db.coll",
                                           "firstBatch": [{"v": "first"}]}},
                     MORE_TO_COME)
            return [first] + stream_builder(mock, req)
        if name == "killCursors":
            return [("reply", {"ok": 1, "cursorsKilled": req["doc"].get("cursors", [])}, 0)]
        return [("reply", {"ok": 1}, 0)]

    return handler


def make_client(cmap_log):
    from pymongo.synchronous.mongo_client import MongoClient

    return MongoClient(
        "mongodb://localhost:27017/?directConnection=true"
        "&serverSelectionTimeoutMS=5000&connectTimeoutMS=5000&socketTimeoutMS=5000",
        event_listeners=[cmap_log],
    )


def pool_of(client):
    server = next(iter(client._topology._servers.values()))
    return server.pool


def report(title, ok, detail):
    print(f"[{'PASS' if ok else 'FAIL'}] {title}\n        {detail}")
    return ok


def scenario_a_early_close():
    cmap = CMapLog()
    set_handler(find_handler(
        lambda mock, req: exhaust_stream(mock, req, [[{"v": "two"}], [{"v": "three"}], [{"v": "four"}]])
    ))
    client = make_client(cmap)
    try:
        cursor = client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)
        assert next(cursor)["v"] == "first"
        pinned = cursor._sock_mgr.conn
        pinned_id = pinned.id
        cursor.close()
        time.sleep(0.2)
        pool = pool_of(client)
        in_pool = any(c.id == pinned_id for c in pool.conns)
        closed_ev = [e for e in cmap.timeline(pinned_id) if e[0] == "closed"]
        pong = client.admin.command("ping")
        ok = (not in_pool) and bool(closed_ev) and pong.get("ok") == 1 and pinned.closed
        return report("A sync early close: unread-MTC socket destroyed",
                      ok, f"pinned={pinned_id} closed={pinned.closed} in_pool={in_pool} "
                          f"close_events={closed_ev} ping_ok={pong.get('ok') == 1}")
    finally:
        client.close()


def scenario_c_truncated_frame():
    cmap = CMapLog()
    set_handler(find_handler(
        lambda mock, req: exhaust_stream(
            mock, req, [[{"v": "two"}], [{"v": "three"}], [{"v": "four"}]], truncate_at=1)
    ))
    client = make_client(cmap)
    try:
        cursor = client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)
        next(cursor)
        pinned = cursor._sock_mgr.conn
        pinned_id = pinned.id
        raised = None
        try:
            list(cursor)
        except BaseException as exc:
            raised = type(exc).__name__
        time.sleep(0.2)
        pool = pool_of(client)
        in_pool = any(c.id == pinned_id for c in pool.conns)
        closed_ev = [e for e in cmap.timeline(pinned_id) if e[0] == "closed"]
        pong = client.admin.command("ping")
        ok = raised is not None and not in_pool and bool(closed_ev) and pong.get("ok") == 1
        return report("C sync truncated frame kills only that socket",
                      ok, f"raised={raised} in_pool={in_pool} close_events={closed_ev} "
                          f"ping_ok={pong.get('ok') == 1}")
    finally:
        client.close()


def scenario_d_midstream_error():
    cmap = CMapLog()
    set_handler(find_handler(
        lambda mock, req: exhaust_stream(
            mock, req, [[{"v": "two"}], [{"v": "three"}], [{"v": "four"}]], error_on=1)
    ))
    client = make_client(cmap)
    try:
        cursor = client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)
        next(cursor)
        pinned = cursor._sock_mgr.conn
        pinned_id = pinned.id
        raised = None
        try:
            list(cursor)
        except OperationFailure as exc:
            raised = f"OperationFailure(code={exc.code})"
        time.sleep(0.2)
        pool = pool_of(client)
        in_pool = any(c.id == pinned_id for c in pool.conns)
        closed_ev = [e for e in cmap.timeline(pinned_id) if e[0] == "closed"]
        pong = client.admin.command("ping")
        ok = raised is not None and not in_pool and bool(closed_ev) and pong.get("ok") == 1
        return report("D sync mid-stream error: socket closed rather than recycled",
                      ok, f"raised={raised} closed={pinned.closed} in_pool={in_pool} "
                          f"close_events={closed_ev} ping_ok={pong.get('ok') == 1}")
    finally:
        client.close()


def scenario_e_unsolicited_continuation():
    cmap = CMapLog()

    def handler(mock, req):
        name = next(iter(req["doc"]))
        if name == "ping":
            good = op_msg_reply(req["req_id"], {"ok": 1}, 0)
            stray = op_msg_reply(req["req_id"] + 9999, {"ok": 1, "stray": True}, 0)
            return [("raw", good + stray)]
        return [("reply", {"ok": 1}, 0)]

    set_handler(handler)
    client = make_client(cmap)
    try:
        first = client.admin.command("ping")
        pool = pool_of(client)
        first_id = next(iter(pool.conns)).id if pool.conns else None
        raised = None
        try:
            client.admin.command("ping")
        except ProtocolError:
            raised = "ProtocolError"
        except ConnectionFailure as exc:
            raised = f"ConnectionFailure({type(exc).__name__})"
        time.sleep(0.2)
        bad_gone = first_id is not None and all(c.id != first_id for c in pool.conns)
        third = client.admin.command("ping")
        ok = raised is not None and bad_gone and third.get("ok") == 1
        return report("E sync stray continuation rejected and isolated",
                      ok, f"rejection={raised} poisoned_removed={bad_gone} third_ping_ok={third.get('ok') == 1}")
    finally:
        client.close()


def scenario_f_normal_streaming():
    cmap = CMapLog()
    set_handler(find_handler(
        lambda mock, req: exhaust_stream(mock, req, [[{"v": "two"}], [{"v": "three"}]])
    ))
    client = make_client(cmap)
    try:
        values = [d["v"] for d in client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)]
        time.sleep(0.2)
        pool = pool_of(client)
        pooled_ids = {c.id for c in pool.conns}
        pong = client.admin.command("ping")
        reused = bool(pooled_ids) and any(c.id in pooled_ids for c in pool.conns)
        ok = values == ["first", "two", "three"] and pong.get("ok") == 1 and reused
        return report("F sync normal streaming preserved and socket pooled",
                      ok, f"docs={values} pooled={pooled_ids} reused={reused} ping_ok={pong.get('ok') == 1}")
    finally:
        client.close()


def main():
    scenarios = [
        scenario_a_early_close,
        scenario_c_truncated_frame,
        scenario_d_midstream_error,
        scenario_e_unsolicited_continuation,
        scenario_f_normal_streaming,
    ]
    results = []
    for scenario in scenarios:
        MOCKS.clear()
        try:
            results.append(scenario())
        except Exception:
            traceback.print_exc()
            results.append(report(scenario.__name__, False, "scenario crashed"))
        time.sleep(0.1)
    print("\n=== SYNC SUMMARY ===")
    print(f"{sum(results)}/{len(results)} scenarios passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    install_factory()
    raise SystemExit(main())
