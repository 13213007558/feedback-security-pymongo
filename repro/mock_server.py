"""In-process MongoDB wire-protocol mock backed by socketpair.

No network bind() is required (the sandbox forbids it): each driver
connection is one end of a socketpair; the server end is driven by a
background thread. The mock parses OP_MSG requests and emits scripted
OP_MSG replies, including moreToCome (exhaust) streams, truncated frames,
error documents, and stray (unsolicited) replies.

Exhaust semantics modeled here: once a find replies with moreToCome the
server keeps pushing OP_MSG batches without any further client request
until the final reply clears moreToCome (and the cursor id becomes 0).
"""
from __future__ import annotations

import bson
import socket
import struct
import threading
import time

OP_MSG = 2013
MORE_TO_COME = 1 << 1


def recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("server saw EOF")
        buf += chunk
    return buf


def read_msg(sock: socket.socket):
    hdr = recv_exact(sock, 16)
    length, req_id, response_to, op_code = struct.unpack("<iiii", hdr)
    body = recv_exact(sock, length - 16)
    flags = struct.unpack("<I", body[:4])[0]
    kind = body[4]
    assert kind == 0, f"unsupported section kind {kind}"
    doc = bson.decode(body[5:])
    return {"req_id": req_id, "op_code": op_code, "flags": flags, "doc": doc}


def op_msg_reply(req_id: int, doc: dict, flags: int = 0, reply_id: int | None = None) -> bytes:
    payload = struct.pack("<I", flags) + b"\x00" + bson.BSON.encode(doc)
    rid = reply_id if reply_id is not None else (int(time.time() * 1e6) % 1_000_000)
    return struct.pack("<iiii", 16 + len(payload), rid, req_id, OP_MSG) + payload


def hello_doc() -> dict:
    return {
        "ok": 1,
        "ismaster": True,
        "minWireVersion": 0,
        "maxWireVersion": 25,
        "connectionId": 1,
        "topologyVersion": {"processId": bson.ObjectId(), "counter": 0},
    }


class MockServer:
    """Scriptable mock for one driver connection.

    handler(mock, request) returns a list of actions:
      ("reply", doc, flags)
      ("raw", bytes)      # arbitrary bytes, e.g. a truncated frame
      ("close",)          # hang up
      ("sleep", seconds)
      ("event", name)     # record a synchronization event
      ("wait", event)     # block until event is set from the test
    Unhandled commands get {"ok": 1}.
    """

    def __init__(self, handler, label: str = "mock"):
        self.handler = handler
        self.label = label
        self.srv_sock, self.cli_sock = socket.socketpair()
        self.events: list = []
        self.sync: dict[str, threading.Event] = {}
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def event(self, name: str) -> threading.Event:
        ev = self.sync.get(name)
        if ev is None:
            ev = threading.Event()
            self.sync[name] = ev
        return ev

    def _send(self, data: bytes) -> None:
        self.srv_sock.sendall(data)
        self.events.append(("send", len(data)))

    def _run(self) -> None:
        try:
            while True:
                req = read_msg(self.srv_sock)
                name = next(iter(req["doc"]))
                self.events.append(("recv", name, req["req_id"], req["flags"]))
                if name in ("hello", "ismaster"):
                    self._send(op_msg_reply(req["req_id"], hello_doc()))
                    continue
                for action in self.handler(self, req):
                    kind = action[0]
                    if kind == "sleep":
                        time.sleep(action[1])
                    elif kind == "wait":
                        self.event(action[1]).wait(timeout=10)
                    elif kind == "event":
                        self.event(action[1]).set()
                    elif kind == "reply":
                        _, doc, flags = action
                        self._send(op_msg_reply(req["req_id"], doc, flags))
                    elif kind == "raw":
                        self._send(action[1])
                    elif kind == "close":
                        self.srv_sock.shutdown(socket.SHUT_RDWR)
                        self.srv_sock.close()
                        return
        except Exception as exc:
            self.events.append(("exit", repr(exc)))

    def close(self) -> None:
        for sock in (self.srv_sock, self.cli_sock):
            try:
                sock.close()
            except OSError:
                pass


def cursor_doc(cursor_id: int, batch_key: str, batch: list, final: bool = False) -> dict:
    return {
        "ok": 1,
        "cursor": {"id": 0 if final else cursor_id, "ns": "db.coll", batch_key: batch},
    }
