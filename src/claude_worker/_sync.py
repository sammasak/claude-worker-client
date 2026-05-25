from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Iterator

from .client import ClaudeWorkerClient
from .models import Goal, GoalStatus, HealthResponse, StreamEvent


def _run_async(coro):
    """Run an async coroutine synchronously via thread-executor dispatch.

    Each call runs its own event loop in a ThreadPoolExecutor thread.
    This is the pattern used by Modal's synchronicity library to expose
    synchronous wrappers over an async-first API.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


class SyncClaudeWorkerClient:
    """Synchronous wrapper over :class:`ClaudeWorkerClient`.

    Identical interface to the async client; every method blocks until
    the underlying coroutine completes, dispatched to a thread executor.

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

    def __enter__(self) -> SyncClaudeWorkerClient:
        _run_async(self._async.__aenter__())
        return self

    def __exit__(self, *args: object) -> None:
        _run_async(self._async.__aexit__(*args))

    def health(self) -> HealthResponse:
        return _run_async(self._async.health())

    def list_goals(self) -> list[Goal]:
        return _run_async(self._async.list_goals())

    def create_goal(self, goal: str) -> Goal:
        return _run_async(self._async.create_goal(goal))

    def update_goal(
        self,
        goal_id: str,
        *,
        status: GoalStatus | None = None,
        result: str | None = None,
    ) -> Goal:
        return _run_async(self._async.update_goal(goal_id, status=status, result=result))

    def stream_events(self, goal_id: str) -> Iterator[StreamEvent]:
        """Synchronously iterate over SSE events for a goal.

        Runs the async generator in a dedicated thread, bridging events
        back to the calling thread via a queue.
        """
        import queue as queue_module

        event_queue: queue_module.SimpleQueue[StreamEvent | BaseException | None] = (
            queue_module.SimpleQueue()
        )

        async def _collect() -> None:
            try:
                async for event in self._async.stream_events(goal_id):
                    event_queue.put(event)
            except Exception as exc:
                event_queue.put(exc)
            finally:
                event_queue.put(None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, _collect())
            while True:
                item = event_queue.get()
                if item is None:
                    break
                if isinstance(item, BaseException):
                    raise item
                yield item
            future.result()

    def submit_and_stream(self, goal: str) -> Iterator[StreamEvent]:
        """Create a goal and synchronously stream its events."""
        created = self.create_goal(goal)
        yield from self.stream_events(created.id)
