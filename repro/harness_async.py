"""Exhaust-cursor early close / cancellation vs. connection-pool reuse.

Each scenario records CMAP pin/checkin/close events and inspects the actual
pool contents, then issues an unrelated command to prove the next borrower
receives its own response rather than a leftover moreToCome batch.

Scenarios:
  A. Exhaust cursor closed while moreToCome batches remain unread.
  B. Asyncio task cancelled while blocked on the next exhaust batch.
  C. A truncated OP_MSG frame appears mid-stream.
  D. An error document replaces an intermediate moreToCome response.
  E. Negative control: a non-exhaust request gets an extra OP_MSG whose
     response_to answers a different request id.
  F. Positive control: a fully consumed exhaust stream keeps normal
     streaming behavior and returns the socket to the pool for reuse.
"""
from __future__ import annotations

import asyncio
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pymongo.asynchronous.pool as pool_mod
from pymongo.network_layer import AsyncNetworkingInterface, PyMongoProtocol
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
    async def fake_factory(address, options, ssl_session_cache=None):
        mock = MockServer(_CURRENT_HANDLER[0], label=f"conn{len(MOCKS)}")
        MOCKS.append(mock)
        loop = asyncio.get_running_loop()
        transport, protocol = await loop.create_connection(
            lambda: PyMongoProtocol(timeout=options.socket_timeout), sock=mock.cli_sock
        )
        return AsyncNetworkingInterface((transport, protocol))

    pool_mod._configured_protocol_interface = fake_factory


def set_handler(fn):
    _CURRENT_HANDLER[0] = fn


def exhaust_stream(mock, req, batches, error_on=None, truncate_at=None, hold_event=None):
    """All moreToCome batches pushed right after the find, final clears MTC."""
    actions = []
    if hold_event:
        # Let the test confirm the client is blocked, then release the stream.
        actions += [("event", f"{hold_event}-entered"), ("wait", hold_event)]
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


async def make_client(cmap_log):
    from pymongo.asynchronous.mongo_client import AsyncMongoClient

    return AsyncMongoClient(
        "mongodb://localhost:27017/?directConnection=true"
        "&serverSelectionTimeoutMS=5000&connectTimeoutMS=5000&socketTimeoutMS=5000",
        event_listeners=[cmap_log],
    )


def pool_of(client):
    server = next(iter(client._topology._servers.values()))
    return server.pool


def data_mock():
    """The mock backing the data (non-SDAM) connection."""
    for mock in MOCKS:
        if any(e[1] == "find" for e in mock.events):
            return mock
    return MOCKS[-1]


def report(title, ok, detail):
    print(f"[{'PASS' if ok else 'FAIL'}] {title}\n        {detail}")
    return ok


async def scenario_a_early_close():
    cmap = CMapLog()
    set_handler(find_handler(
        lambda mock, req: exhaust_stream(mock, req, [[{"v": "two"}], [{"v": "three"}], [{"v": "four"}]])
    ))
    client = await make_client(cmap)
    try:
        cursor = client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)
        assert (await cursor.next())["v"] == "first"
        pinned = cursor._sock_mgr.conn
        pinned_id = pinned.id
        await cursor.close()
        await asyncio.sleep(0.2)
        pool = pool_of(client)
        in_pool = any(c.id == pinned_id for c in pool.conns)
        closed_ev = [e for e in cmap.timeline(pinned_id) if e[0] == "closed"]
        pong = await client.admin.command("ping")
        ok = (not in_pool) and bool(closed_ev) and pong.get("ok") == 1 and pinned.closed
        return report("A early close: unread-MTC socket is destroyed, not pooled",
                      ok, f"pinned={pinned_id} conn.closed={pinned.closed} in_pool_after_close={in_pool} "
                          f"close_events={closed_ev} next_ping_ok={pong.get('ok') == 1}")
    finally:
        await client.close()


async def scenario_b_cancellation():
    cmap = CMapLog()
    set_handler(find_handler(
        lambda mock, req: exhaust_stream(
            mock, req, [[{"v": "two"}], [{"v": "three"}]], hold_event="release")
    ))
    # Reliable sync point: fire only once the cursor is blocked inside the
    # actual receive for the next moreToCome batch (request_id is None).
    read_started = asyncio.Event()
    watched_fd = set()
    original_receive = pool_mod.AsyncConnection.receive_message

    async def receive_traced(self, request_id):
        fd = self.conn.get_conn.transport.get_extra_info("socket").fileno()
        if fd in watched_fd and request_id is None:
            read_started.set()
        return await original_receive(self, request_id)

    pool_mod.AsyncConnection.receive_message = receive_traced
    client = await make_client(cmap)
    try:
        cursor = client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)
        assert (await cursor.next())["v"] == "first"
        pinned = cursor._sock_mgr.conn
        pinned_id = pinned.id
        watched_fd.add(pinned.conn.get_conn.transport.get_extra_info("socket").fileno())
        mock = data_mock()

        task = asyncio.create_task(cursor.to_list())
        assert mock.event("release-entered").wait(timeout=5), "mock never entered hold"
        await asyncio.wait_for(read_started.wait(), timeout=5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        mock.event("release").set()
        await asyncio.sleep(0.3)
        pool = pool_of(client)
        in_pool = any(c.id == pinned_id for c in pool.conns)
        closed_ev = [e for e in cmap.timeline(pinned_id) if e[0] == "closed"]
        pong = await client.admin.command("ping")
        ok = (not in_pool) and bool(closed_ev) and pong.get("ok") == 1 and pinned.closed
        return report("B task cancellation: blocked exhaust socket is destroyed",
                      ok, f"pinned={pinned_id} conn.closed={pinned.closed} in_pool={in_pool} "
                          f"close_events={closed_ev} next_ping_ok={pong.get('ok') == 1}")
    finally:
        pool_mod.AsyncConnection.receive_message = original_receive
        await client.close()


async def scenario_c_truncated_frame():
    cmap = CMapLog()
    set_handler(find_handler(
        lambda mock, req: exhaust_stream(
            mock, req, [[{"v": "two"}], [{"v": "three"}], [{"v": "four"}]], truncate_at=1)
    ))
    client = await make_client(cmap)
    try:
        cursor = client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)
        await cursor.next()
        pinned = cursor._sock_mgr.conn
        pinned_id = pinned.id
        raised = None
        try:
            await cursor.to_list()
        except BaseException as exc:
            raised = type(exc).__name__
        await asyncio.sleep(0.2)
        pool = pool_of(client)
        in_pool = any(c.id == pinned_id for c in pool.conns)
        closed_ev = [e for e in cmap.timeline(pinned_id) if e[0] == "closed"]
        pong = await client.admin.command("ping")
        ok = raised is not None and not in_pool and bool(closed_ev) and pong.get("ok") == 1
        return report("C truncated frame: only that socket dies, fresh one is sound",
                      ok, f"raised={raised} pinned={pinned_id} in_pool={in_pool} "
                          f"close_events={closed_ev} next_ping_ok={pong.get('ok') == 1}")
    finally:
        await client.close()


async def scenario_d_midstream_error():
    cmap = CMapLog()
    set_handler(find_handler(
        lambda mock, req: exhaust_stream(
            mock, req, [[{"v": "two"}], [{"v": "three"}], [{"v": "four"}]], error_on=1)
    ))
    client = await make_client(cmap)
    try:
        cursor = client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)
        await cursor.next()
        pinned = cursor._sock_mgr.conn
        pinned_id = pinned.id
        raised = None
        try:
            await cursor.to_list()
        except OperationFailure as exc:
            raised = f"OperationFailure(code={exc.code})"
        await asyncio.sleep(0.2)
        pool = pool_of(client)
        in_pool = any(c.id == pinned_id for c in pool.conns)
        closed_ev = [e for e in cmap.timeline(pinned_id) if e[0] == "closed"]
        pong = await client.admin.command("ping")
        ok = raised is not None and not in_pool and bool(closed_ev) and pong.get("ok") == 1
        return report("D mid-stream error document: socket closed rather than recycled",
                      ok, f"raised={raised} pinned={pinned_id} in_pool={in_pool} "
                          f"close_events={closed_ev} next_ping_ok={pong.get('ok') == 1}")
    finally:
        await client.close()


async def scenario_e_unsolicited_continuation():
    cmap = CMapLog()

    def handler(mock, req):
        name = next(iter(req["doc"]))
        if name == "ping":
            good = op_msg_reply(req["req_id"], {"ok": 1}, 0)
            # Stray continuation answering a request id that does not exist.
            stray = op_msg_reply(req["req_id"] + 9999, {"ok": 1, "stray": True}, 0)
            return [("raw", good + stray)]
        return [("reply", {"ok": 1}, 0)]

    set_handler(handler)
    client = await make_client(cmap)
    try:
        first = await client.admin.command("ping")
        pool = pool_of(client)
        first_conn = next(iter(pool.conns)) if pool.conns else None
        first_id = first_conn.id if first_conn else None
        raised = None
        try:
            await client.admin.command("ping")
        except ProtocolError:
            raised = "ProtocolError"
        except ConnectionFailure as exc:
            raised = f"ConnectionFailure({type(exc).__name__})"
        await asyncio.sleep(0.2)
        bad_gone = first_id is not None and all(c.id != first_id for c in pool.conns)
        third = await client.admin.command("ping")
        ok = raised is not None and bad_gone and third.get("ok") == 1
        return report("E non-exhaust stray continuation is rejected and isolated",
                      ok, f"first_ping_ok={first.get('ok') == 1} rejection={raised} "
                          f"poisoned_conn_removed={bad_gone} third_ping_ok={third.get('ok') == 1}")
    finally:
        await client.close()


async def scenario_f_normal_streaming():
    cmap = CMapLog()
    set_handler(find_handler(
        lambda mock, req: exhaust_stream(mock, req, [[{"v": "two"}], [{"v": "three"}]])
    ))
    client = await make_client(cmap)
    try:
        cursor = client.db.coll.find({}, cursor_type=CursorType.EXHAUST, batch_size=1)
        docs = await cursor.to_list()
        values = [d["v"] for d in docs]
        await asyncio.sleep(0.2)
        pool = pool_of(client)
        pooled_ids = {c.id for c in pool.conns}
        pong = await client.admin.command("ping")
        reused = pooled_ids and any(c.id in pooled_ids for c in pool.conns)
        ok = values == ["first", "two", "three"] and pong.get("ok") == 1 and reused
        return report("F normal exhaust streaming preserved and socket pooled",
                      ok, f"docs={values} pooled_conn_ids={pooled_ids} "
                          f"reused_after_ping={reused} ping_ok={pong.get('ok') == 1}")
    finally:
        await client.close()


async def main():
    scenarios = [
        scenario_a_early_close,
        scenario_b_cancellation,
        scenario_c_truncated_frame,
        scenario_d_midstream_error,
        scenario_e_unsolicited_continuation,
        scenario_f_normal_streaming,
    ]
    results = []
    for scenario in scenarios:
        MOCKS.clear()
        try:
            results.append(await scenario())
        except Exception:
            traceback.print_exc()
            results.append(report(scenario.__name__, False, "scenario crashed"))
        await asyncio.sleep(0.1)
    print("\n=== SUMMARY ===")
    print(f"{sum(results)}/{len(results)} scenarios passed")
    return 0 if all(results) else 1


if __name__ == "__main__":
    install_factory()
    raise SystemExit(asyncio.run(main()))
