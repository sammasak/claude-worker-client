# claude-worker-client

Async Python client for the [claude-worker](https://github.com/sammasak/claude-worker) agent runtime API.

## Installation

```bash
pip install claude-worker-client
```

## Quick start

### Async

```python
import asyncio
from claude_worker import ClaudeWorkerClient

async def main():
    async with ClaudeWorkerClient("http://worker:4200", api_key="your-key") as client:
        goal = await client.create_goal("Build a FastAPI service with health endpoint")
        async for event in client.stream_events(goal.id):
            print(event.data)

asyncio.run(main())
```

### Sync

```python
from claude_worker import SyncClaudeWorkerClient

with SyncClaudeWorkerClient("http://worker:4200", api_key="your-key") as client:
    goal = client.create_goal("Build a FastAPI service with health endpoint")
    for event in client.stream_events(goal.id):
        print(event.data)
```

## API

### `ClaudeWorkerClient`

Async context manager. All methods are coroutines.

| Method | Description |
|--------|-------------|
| `health()` | Get worker health and queue status |
| `list_goals()` | List all goals |
| `create_goal(goal)` | Submit a new goal (raises `QueueFull` if queue is at capacity) |
| `update_goal(id, *, status, result)` | Update goal status or result |
| `stream_events(goal_id)` | Async-iterate SSE events with automatic reconnection and exponential backoff |
| `submit_and_stream(goal)` | Create a goal and immediately stream its events |

### `SyncClaudeWorkerClient`

Synchronous wrapper over `ClaudeWorkerClient`. Uses a persistent background event loop — see [Design notes](#design-notes) below.

### Models

All response types are Pydantic v2 `BaseModel` instances:

- `Goal` — agent goal with status, timestamps, and result
- `GoalStatus` — `StrEnum`: `pending`, `in_progress`, `done`, `failed`
- `HealthResponse` — worker health including queue depth and running state
- `StreamEvent` — a single SSE event with `event_type` and `data`

### Exceptions

| Exception | When raised |
|-----------|-------------|
| `QueueFull` | `create_goal` when the server returns 429 |
| `GoalNotFound` | `update_goal` when the goal ID does not exist |
| `GoalFailed` | `stream_events` when the server signals `[FAILED:code]` |

## SSE stream

The event stream reconnects automatically on connection failures with exponential backoff (default: 1s initial, 30s cap, 5 retries). Events with `event_type == "hook"` carry structured progress messages from the agent's tool hooks. The stream terminates on `[DONE]` or raises `GoalFailed` on `[FAILED:]`.

## Development

```bash
uv sync --extra dev
pytest
ruff check src/ tests/
```

---

## Design notes

### Why a persistent background event loop, not `asyncio.run()` per call

`SyncClaudeWorkerClient` needs to expose a synchronous API while reusing a single
`httpx.AsyncClient` across calls. The naive approach — calling `asyncio.run(coro)` for
each method — fails silently:

```python
# WRONG — each asyncio.run() creates a new event loop, enters the __aenter__,
# then immediately destroys that loop when it exits. The next call opens a fresh
# loop. self._client is set in __aenter__ on loop A; when loop B starts it tries
# to reuse the same httpx.AsyncClient, which is now bound to a closed loop.
# The result: RuntimeError or silently dropped requests.

async with client:               # loop A: opens AsyncClient
    await client.health()        # loop B: tries to reuse AsyncClient bound to A
```

The correct approach is what [Modal's `synchronicity` library](https://github.com/modal-labs/synchronicity)
uses: a single event loop that lives for the lifetime of the context manager, running
`run_forever()` in a daemon thread. Calls are dispatched into it from the main thread
using `asyncio.run_coroutine_threadsafe`, which is thread-safe and returns a
`concurrent.futures.Future` you can block on:

```python
def __enter__(self):
    self._loop = asyncio.new_event_loop()
    self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
    self._thread.start()
    # __aenter__ runs on the background loop — httpx.AsyncClient is opened there
    asyncio.run_coroutine_threadsafe(self._async.__aenter__(), self._loop).result()
    return self

def _run(self, coro):
    # Every subsequent call dispatches here. Same loop, same AsyncClient.
    return asyncio.run_coroutine_threadsafe(coro, self._loop).result()
```

The thread is a daemon so it does not prevent process exit. On `__exit__`,
`loop.call_soon_threadsafe(loop.stop)` schedules a stop from the main thread
(calling `loop.stop()` directly from outside the loop is not safe), then we join
the thread and close the loop.

One subtle issue: if `_run()` is called outside the context manager, the coroutine
was passed in but will never be awaited. Python will emit a `ResourceWarning: coroutine
'X' was never awaited`. The fix is to explicitly close it before raising:

```python
def _run(self, coro):
    if self._loop is None:
        coro.close()  # prevent ResourceWarning: coroutine never awaited
        raise RuntimeError("Use SyncClaudeWorkerClient as a context manager")
    return asyncio.run_coroutine_threadsafe(coro, self._loop).result()
```

### Streaming in a sync-to-async bridge

`stream_events` is an async generator, which can't be driven by `run_coroutine_threadsafe`
directly (that function expects a single coroutine, not a generator). The solution is to
drain the generator into a `queue.SimpleQueue` on the background loop, then consume the
queue on the main thread:

```python
def stream_events(self, goal_id):
    event_queue: queue.SimpleQueue[StreamEvent | BaseException | None] = queue.SimpleQueue()

    async def _drain():
        try:
            async for event in self._async.stream_events(goal_id):
                event_queue.put(event)
        except Exception as exc:
            event_queue.put(exc)
        finally:
            event_queue.put(None)   # sentinel

    asyncio.run_coroutine_threadsafe(_drain(), self._loop)

    while True:
        item = event_queue.get()    # blocks main thread until next event
        if item is None:
            return
        if isinstance(item, BaseException):
            raise item
        yield item
```

`SimpleQueue` (not `Queue`) is used because it has no maximum size and its `put`/`get`
are individually thread-safe without a lock.

### SSE parsing: RFC 8895 §9.2

The Server-Sent Events spec (RFC 8895 §9.2) says `data:` fields accumulate across
consecutive lines and the event is dispatched on the blank line that ends the block.
A naive line-by-line parser that emits one event per `data:` line will incorrectly
split multi-line payloads:

```
data: {"type": "progress",
data: "message": "compiling..."}

```

The above is a single event whose data is `{"type": "progress",\n"message": "compiling..."}`.
A one-event-per-`data:`-line parser would emit two broken partial events.

The correct implementation accumulates `data:` lines in a list and joins on dispatch:

```python
event_type = "message"
data_lines: list[str] = []
async for raw_line in resp.aiter_lines():
    if raw_line.startswith("event:"):
        event_type = raw_line[len("event:"):].strip()
    elif raw_line.startswith("data:"):
        data_lines.append(raw_line[len("data:"):].strip())
    elif not raw_line and data_lines:           # blank line = dispatch
        data = "\n".join(data_lines)
        data_lines.clear()
        event = StreamEvent(event_type=event_type, data=data)
        event_type = "message"
        yield event
```

### Timeout split: streaming vs. non-streaming

SSE streams require an unbounded read timeout (the connection stays open indefinitely
waiting for events) but a bounded connect timeout (we do not want to hang forever
waiting for the TCP handshake). Non-streaming calls (health, create_goal, etc.) use
the single `timeout` parameter from the constructor.

`stream_events` therefore creates its own `httpx.AsyncClient` with
`httpx.Timeout(None, connect=10.0)` rather than reusing the shared client:

```python
async with httpx.AsyncClient(timeout=httpx.Timeout(None, connect=10.0)) as stream_client:
    async with stream_client.stream("GET", url, headers=stream_headers) as resp:
        ...
```
