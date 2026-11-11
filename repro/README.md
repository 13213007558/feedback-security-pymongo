# Exhaust-cursor connection-reuse reproduction

These scripts need no MongoDB server and no listening socket: each driver
connection is one end of a `socketpair`, with the other end driven by a
threaded OP_MSG mock in `mock_server.py`. This lets the sandbox (which blocks
`bind()`) reproduce the exact pin / close / checkin behavior.

## Run

```bash
# async driver (6 scenarios, incl. task cancellation)
.venv/bin/python repro/harness_async.py

# generated synchronous driver (5 scenarios; cancellation is async-only)
.venv/bin/python repro/harness_sync.py
```

## What each scenario proves

- A: exhaust cursor closed while moreToCome batches remain unread.
- B: asyncio task cancelled while blocked receiving the next batch.
- C: a truncated OP_MSG frame in the middle of the stream.
- D: an error document replacing an intermediate moreToCome response.
- E: negative control - a non-exhaust request gets an extra OP_MSG whose
  `response_to` answers a different request id.
- F: positive control - a fully consumed stream still returns the socket to
  the pool (normal exhaust streaming is preserved).

Every scenario checks the CMAP pin/close/checkin timeline, the real pool
contents, and the response of an unrelated command issued afterwards.

## Finding

Before the fix, scenario D left the pinned connection open and returned it to
the pool with unread moreToCome bytes still buffered. The next borrower then
raised `ProtocolError: Got response id ... but expected ...` and could have
consumed another cursor's data. The cursor error handler now forces the
exhaust connection to be closed (`cursor.py`), and the pool independently
refuses to check in any connection flagged `exhaust_failed`
(`pymongo/pool_shared.py::_checkin_apply`).
