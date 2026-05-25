"""Tests for ClaudeWorkerClient using respx for HTTP mocking."""

import httpx
import pytest
import respx

from claude_worker import (
    ClaudeWorkerClient,
    Goal,
    GoalNotFound,
    GoalStatus,
    QueueFull,
)

BASE_URL = "http://worker.test:4200"
API_KEY = "test-key"


@pytest.fixture
def client() -> ClaudeWorkerClient:
    return ClaudeWorkerClient(BASE_URL, api_key=API_KEY)


@pytest.fixture
def goal_payload() -> dict:
    return {
        "id": "abc-123",
        "goal": "Build a status page",
        "status": "pending",
        "created_at": "2026-01-01T00:00:00Z",
        "started_at": None,
        "completed_at": None,
        "reviewed_at": None,
        "result": None,
    }


@respx.mock
async def test_health(client: ClaudeWorkerClient) -> None:
    respx.get(f"{BASE_URL}/health").mock(
        return_value=httpx.Response(
            200,
            json={
                "status": "ok",
                "claude_running": True,
                "pending_goals": 2,
                "in_progress_goals": 1,
            },
        )
    )
    async with client:
        health = await client.health()
    assert health.status == "ok"
    assert health.claude_running is True
    assert health.pending_goals == 2


@respx.mock
async def test_list_goals_empty(client: ClaudeWorkerClient) -> None:
    respx.get(f"{BASE_URL}/goals").mock(return_value=httpx.Response(200, json=[]))
    async with client:
        goals = await client.list_goals()
    assert goals == []


@respx.mock
async def test_create_goal(client: ClaudeWorkerClient, goal_payload: dict) -> None:
    respx.post(f"{BASE_URL}/goals").mock(return_value=httpx.Response(201, json=goal_payload))
    async with client:
        goal = await client.create_goal("Build a status page")
    assert isinstance(goal, Goal)
    assert goal.id == "abc-123"
    assert goal.status == GoalStatus.PENDING


@respx.mock
async def test_create_goal_queue_full(client: ClaudeWorkerClient) -> None:
    respx.post(f"{BASE_URL}/goals").mock(
        return_value=httpx.Response(429, json={"error": "queue full", "pending": 50})
    )
    async with client:
        with pytest.raises(QueueFull):
            await client.create_goal("overflow")


@respx.mock
async def test_update_goal_not_found(client: ClaudeWorkerClient) -> None:
    respx.put(f"{BASE_URL}/goals/missing-id").mock(return_value=httpx.Response(404))
    async with client:
        with pytest.raises(GoalNotFound):
            await client.update_goal("missing-id", status=GoalStatus.DONE)


@respx.mock
async def test_update_goal_status(client: ClaudeWorkerClient, goal_payload: dict) -> None:
    done_payload = {**goal_payload, "status": "done", "completed_at": "2026-01-01T01:00:00Z"}
    respx.put(f"{BASE_URL}/goals/abc-123").mock(
        return_value=httpx.Response(200, json=done_payload)
    )
    async with client:
        updated = await client.update_goal("abc-123", status=GoalStatus.DONE)
    assert updated.status == GoalStatus.DONE
    assert updated.is_terminal


async def test_stream_events_done(client: ClaudeWorkerClient) -> None:
    sse_body = (
        "data: HOOK:{\"type\":\"progress\",\"message\":\"Starting...\"}\n\n"
        "data: [DONE]\n\n"
    )
    with respx.mock:
        respx.get(f"{BASE_URL}/goals/abc-123/stream").mock(
            return_value=httpx.Response(200, text=sse_body, headers={"content-type": "text/event-stream"})
        )
        events = []
        async with client:
            async for event in client.stream_events("abc-123"):
                events.append(event)

    assert len(events) == 1
    assert "Starting" in events[0].data


async def test_stream_events_failure(client: ClaudeWorkerClient) -> None:
    from claude_worker import GoalFailed
    sse_body = "data: [FAILED:1]\n\n"
    with respx.mock:
        respx.get(f"{BASE_URL}/goals/abc-123/stream").mock(
            return_value=httpx.Response(200, text=sse_body, headers={"content-type": "text/event-stream"})
        )
        async with client:
            with pytest.raises(GoalFailed) as exc_info:
                async for _ in client.stream_events("abc-123"):
                    pass
    assert exc_info.value.code == "1"


def test_goal_is_terminal() -> None:
    goal = Goal(
        id="x",
        goal="test",
        status=GoalStatus.DONE,
        created_at="2026-01-01T00:00:00Z",
    )
    assert goal.is_terminal

    pending = Goal(
        id="y",
        goal="test",
        status=GoalStatus.PENDING,
        created_at="2026-01-01T00:00:00Z",
    )
    assert not pending.is_terminal


async def test_stream_events_retries_on_transport_error(client: ClaudeWorkerClient) -> None:
    """On a transient connection failure, stream_events reconnects and resets the backoff counter."""
    sse_success = "data: HOOK:{\"type\":\"progress\",\"message\":\"Connected\"}\n\ndata: [DONE]\n\n"

    call_count = 0

    def side_effect(request):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise httpx.ConnectError("simulated transient failure")
        return httpx.Response(
            200, text=sse_success, headers={"content-type": "text/event-stream"}
        )

    with respx.mock:
        respx.get(f"{BASE_URL}/goals/retry-test/stream").mock(side_effect=side_effect)
        events = []
        async with ClaudeWorkerClient(
            BASE_URL, api_key=API_KEY, initial_backoff=0.001, max_backoff=0.01
        ) as client:
            async for event in client.stream_events("retry-test"):
                events.append(event)

    assert call_count == 2, "Expected exactly one retry after the transient failure"
    assert len(events) == 1
    assert "Connected" in events[0].data


async def test_stream_events_raises_after_max_retries(client: ClaudeWorkerClient) -> None:
    """After max_retries consecutive failures, stream_events re-raises the transport error."""
    with respx.mock:
        respx.get(f"{BASE_URL}/goals/fail-all/stream").mock(
            side_effect=httpx.ConnectError("persistent failure")
        )
        with pytest.raises(httpx.ConnectError):
            async with ClaudeWorkerClient(
                BASE_URL, api_key=API_KEY, max_retries=2, initial_backoff=0.001
            ) as client:
                async for _ in client.stream_events("fail-all"):
                    pass


async def test_stream_events_multiline_data(client: ClaudeWorkerClient) -> None:
    """Multi-line SSE data fields (RFC 8895 §9.2) must be joined and emitted as one event."""
    sse_body = (
        "data: line1\n"
        "data: line2\n"
        "data: line3\n"
        "\n"
        "data: [DONE]\n"
        "\n"
    )
    with respx.mock:
        respx.get(f"{BASE_URL}/goals/multiline/stream").mock(
            return_value=httpx.Response(200, text=sse_body, headers={"content-type": "text/event-stream"})
        )
        events = []
        async with client:
            async for event in client.stream_events("multiline"):
                events.append(event)

    assert len(events) == 1, "Three data lines in one SSE block must produce exactly one event"
    assert events[0].data == "line1\nline2\nline3"
