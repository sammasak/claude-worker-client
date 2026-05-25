from __future__ import annotations

import asyncio
import queue
import threading
from collections.abc import Iterator

from .client import ClaudeWorkerClient
from .models import Goal, GoalStatus, HealthResponse, StreamEvent


class SyncClaudeWorkerClient:
    """Synchronous wrapper over :class:`ClaudeWorkerClient`.

    Runs a single persistent event loop in a background daemon thread.
    All async calls are dispatched into that loop via
    ``asyncio.run_coroutine_threadsafe``, which is the same pattern used
    by Modal's ``synchronicity`` library.

    Usage::

        with SyncClaudeWorkerClient("http://worker:4200", api_key="...") as client:
            goal = client.create_goal("Build a status page")
            for event in client.stream_events(goal.id):
                print(event.data)
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout: float = 30.0,
        max_retries: int = 5,
        initial_backoff: float = 1.0,
        max_backoff: float = 30.0,
    ) -> None:
        self._async = ClaudeWorkerClient(
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
            max_retries=max_retries,
            initial_backoff=initial_backoff,
            max_backoff=max_backoff,
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None

    def _run(self, coro):
        """Dispatch a coroutine to the persistent background event loop and block for the result."""
        if self._loop is None:
            coro.close()  # prevent ResourceWarning: coroutine never awaited
            raise RuntimeError("Use SyncClaudeWorkerClient as a context manager")
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def __enter__(self) -> SyncClaudeWorkerClient:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._run(self._async.__aenter__())
        return self

    def __exit__(self, *args: object) -> None:
        try:
            self._run(self._async.__aexit__(*args))
        finally:
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread is not None:
                self._thread.join(timeout=10)
            if self._loop is not None:
                self._loop.close()
            self._loop = None
            self._thread = None

    def health(self) -> HealthResponse:
        return self._run(self._async.health())

    def list_goals(self) -> list[Goal]:
        return self._run(self._async.list_goals())

    def create_goal(self, goal: str) -> Goal:
        return self._run(self._async.create_goal(goal))

    def update_goal(
        self,
        goal_id: str,
        *,
        status: GoalStatus | None = None,
        result: str | None = None,
    ) -> Goal:
        return self._run(self._async.update_goal(goal_id, status=status, result=result))

    def stream_events(self, goal_id: str) -> Iterator[StreamEvent]:
        """Synchronously iterate over SSE events for a goal.

        Runs the async generator in the persistent background event loop,
        bridging events back to the calling thread via a thread-safe queue.
        """
        event_queue: queue.SimpleQueue[StreamEvent | BaseException | None] = queue.SimpleQueue()

        async def _drain() -> None:
            try:
                async for event in self._async.stream_events(goal_id):
                    event_queue.put(event)
            except Exception as exc:
                event_queue.put(exc)
            finally:
                event_queue.put(None)

        # self._loop is guaranteed non-None here: stream_events can only be reached
        # through the context manager (__enter__ sets _loop before returning self),
        # but the type checker cannot see that threading invariant.
        asyncio.run_coroutine_threadsafe(_drain(), self._loop)  # type: ignore[arg-type]

        while True:
            item = event_queue.get()
            if item is None:
                return
            if isinstance(item, BaseException):
                raise item
            yield item

    def submit_and_stream(self, goal: str) -> Iterator[StreamEvent]:
        """Create a goal and synchronously stream its events."""
        created = self.create_goal(goal)
        yield from self.stream_events(created.id)
