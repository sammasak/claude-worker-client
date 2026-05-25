from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx

from .models import Goal, GoalStatus, HealthResponse, StreamEvent


class ClaudeWorkerError(Exception):
    pass


class GoalNotFound(ClaudeWorkerError):
    pass


class QueueFull(ClaudeWorkerError):
    pass


class GoalFailed(ClaudeWorkerError):
    def __init__(self, code: str | None = None) -> None:
        self.code = code
        super().__init__(f"Goal failed with code: {code}")


class ClaudeWorkerClient:
    """Async client for the claude-worker agent runtime API.

    Usage::

        async with ClaudeWorkerClient("http://worker:4200", api_key="...") as client:
            goal = await client.create_goal("Build a status page")
            async for event in client.stream_events(goal.id):
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
        self._base_url = base_url.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }
        self._timeout = timeout
        self._max_retries = max_retries
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> ClaudeWorkerClient:
        self._client = httpx.AsyncClient(headers=self._headers, timeout=self._timeout)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Use ClaudeWorkerClient as an async context manager")
        return self._client

    async def health(self) -> HealthResponse:
        resp = await self._http().get(f"{self._base_url}/health")
        resp.raise_for_status()
        return HealthResponse.model_validate(resp.json())

    async def list_goals(self) -> list[Goal]:
        resp = await self._http().get(f"{self._base_url}/goals")
        resp.raise_for_status()
        return [Goal.model_validate(g) for g in resp.json()]

    async def create_goal(self, goal: str) -> Goal:
        resp = await self._http().post(f"{self._base_url}/goals", json={"goal": goal})
        if resp.status_code == 429:
            raise QueueFull(resp.json().get("error", "queue full"))
        resp.raise_for_status()
        return Goal.model_validate(resp.json())

    async def update_goal(
        self,
        goal_id: str,
        *,
        status: GoalStatus | None = None,
        result: str | None = None,
    ) -> Goal:
        payload: dict[str, object] = {}
        if status is not None:
            payload["status"] = status
        if result is not None:
            payload["result"] = result
        resp = await self._http().put(f"{self._base_url}/goals/{goal_id}", json=payload)
        if resp.status_code == 404:
            raise GoalNotFound(goal_id)
        resp.raise_for_status()
        return Goal.model_validate(resp.json())

    async def stream_events(self, goal_id: str) -> AsyncIterator[StreamEvent]:
        """Stream SSE events for a goal, reconnecting with exponential backoff on failure.

        Yields :class:`StreamEvent` instances until the server signals completion
        (``[DONE]``) or raises :class:`GoalFailed` on failure.
        """
        retry_count = 0
        backoff = self._initial_backoff

        url = f"{self._base_url}/goals/{goal_id}/stream"
        stream_headers = {**self._headers, "Accept": "text/event-stream"}

        while True:
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(None, connect=10.0)
                ) as stream_client:
                    async with stream_client.stream("GET", url, headers=stream_headers) as resp:
                        resp.raise_for_status()
                        retry_count = 0
                        backoff = self._initial_backoff

                        event_type = "message"
                        async for raw_line in resp.aiter_lines():
                            if raw_line.startswith("event:"):
                                event_type = raw_line[len("event:") :].strip()
                            elif raw_line.startswith("data:"):
                                data = raw_line[len("data:") :].strip()
                                event = StreamEvent(event_type=event_type, data=data)
                                if event.is_done:
                                    return
                                if event.is_failed:
                                    raise GoalFailed(event.failure_code)
                                yield event
                                event_type = "message"
                            elif not raw_line:
                                event_type = "message"
            except GoalFailed:
                raise
            except (httpx.HTTPError, httpx.TransportError):
                retry_count += 1
                if retry_count > self._max_retries:
                    raise
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._max_backoff)

    async def submit_and_stream(self, goal: str) -> AsyncIterator[StreamEvent]:
        """Create a goal and immediately stream its events."""
        created = await self.create_goal(goal)
        async for event in self.stream_events(created.id):
            yield event
