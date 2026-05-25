"""Tests for SyncClaudeWorkerClient and streaming reconnection logic."""

import asyncio
import threading
import time

import httpx
import pytest
import respx

from claude_worker import (
    Goal,
    GoalFailed,
    GoalStatus,
    QueueFull,
    SyncClaudeWorkerClient,
)

BASE_URL = "http://worker.test:4200"
API_KEY = "test-key"


@pytest.fixture
def sync_client() -> SyncClaudeWorkerClient:
    return SyncClaudeWorkerClient(BASE_URL, api_key=API_KEY)


@pytest.fixture
def goal_payload() -> dict:
    return {
        "id": "sync-123",
        "goal": "sync test goal",
        "status": "pending",
        "created_at": "2026-01-01T00:00:00Z",
        "started_at": None,
        "completed_at": None,
        "reviewed_at": None,
        "result": None,
    }


@respx.mock
def test_sync_health(sync_client: SyncClaudeWorkerClient) -> None:
    respx.get(f"{BASE_URL}/health").mock(
        return_value=httpx.Response(
            200,
            json={"status": "ok", "claude_running": False, "pending_goals": 0, "in_progress_goals": 0},
        )
    )
    with sync_client as client:
        health = client.health()
    assert health.status == "ok"
    assert health.claude_running is False


@respx.mock
def test_sync_create_goal(sync_client: SyncClaudeWorkerClient, goal_payload: dict) -> None:
    respx.post(f"{BASE_URL}/goals").mock(return_value=httpx.Response(201, json=goal_payload))
    with sync_client as client:
        goal = client.create_goal("sync test goal")
    assert isinstance(goal, Goal)
    assert goal.id == "sync-123"
    assert goal.status == GoalStatus.PENDING


@respx.mock
def test_sync_queue_full(sync_client: SyncClaudeWorkerClient) -> None:
    respx.post(f"{BASE_URL}/goals").mock(
        return_value=httpx.Response(429, json={"error": "queue full", "pending": 50})
    )
    with sync_client as client:
        with pytest.raises(QueueFull):
            client.create_goal("overflow")


@respx.mock
def test_sync_list_goals(sync_client: SyncClaudeWorkerClient, goal_payload: dict) -> None:
    respx.get(f"{BASE_URL}/goals").mock(return_value=httpx.Response(200, json=[goal_payload]))
    with sync_client as client:
        goals = client.list_goals()
    assert len(goals) == 1
    assert goals[0].id == "sync-123"


def test_sync_background_loop_is_daemon(sync_client: SyncClaudeWorkerClient) -> None:
    """The background event loop thread must be a daemon so it does not block process exit."""
    with sync_client:
        assert sync_client._thread is not None
        assert sync_client._thread.daemon is True


def test_sync_loop_torn_down_after_exit(sync_client: SyncClaudeWorkerClient) -> None:
    """After __exit__, the background loop and thread are cleaned up."""
    with respx.mock:
        respx.get(f"{BASE_URL}/health").mock(
            return_value=httpx.Response(
                200,
                json={"status": "ok", "claude_running": False, "pending_goals": 0, "in_progress_goals": 0},
            )
        )
        with sync_client as client:
            client.health()
            thread = client._thread
        assert thread is not None
        assert not thread.is_alive()
        assert sync_client._loop is None
        assert sync_client._thread is None


def test_sync_stream_events_done(sync_client: SyncClaudeWorkerClient) -> None:
    sse_body = (
        'data: HOOK:{"type":"progress","message":"Working..."}\n\n'
        "data: [DONE]\n\n"
    )
    with respx.mock:
        respx.get(f"{BASE_URL}/goals/sync-123/stream").mock(
            return_value=httpx.Response(
                200, text=sse_body, headers={"content-type": "text/event-stream"}
            )
        )
        events = []
        with sync_client as client:
            for event in client.stream_events("sync-123"):
                events.append(event)

    assert len(events) == 1
    assert "Working" in events[0].data


def test_sync_stream_events_failure(sync_client: SyncClaudeWorkerClient) -> None:
    sse_body = "data: [FAILED:2]\n\n"
    with respx.mock:
        respx.get(f"{BASE_URL}/goals/sync-123/stream").mock(
            return_value=httpx.Response(
                200, text=sse_body, headers={"content-type": "text/event-stream"}
            )
        )
        with sync_client as client:
            with pytest.raises(GoalFailed) as exc_info:
                for _ in client.stream_events("sync-123"):
                    pass
    assert exc_info.value.code == "2"


def test_sync_dispatches_to_single_event_loop(sync_client: SyncClaudeWorkerClient) -> None:
    """All calls within the context manager execute on the same background event loop."""
    loop_ids: list[int] = []

    async def capture_loop() -> int:
        return id(asyncio.get_running_loop())

    with respx.mock:
        respx.get(f"{BASE_URL}/health").mock(
            return_value=httpx.Response(
                200,
                json={"status": "ok", "claude_running": False, "pending_goals": 0, "in_progress_goals": 0},
            )
        )
        with sync_client as client:
            background_loop = client._loop
            assert background_loop is not None
            loop_ids.append(
                asyncio.run_coroutine_threadsafe(capture_loop(), background_loop).result()
            )
            client.health()
            loop_ids.append(
                asyncio.run_coroutine_threadsafe(capture_loop(), background_loop).result()
            )

    assert len(set(loop_ids)) == 1, "All calls should run on the same event loop"


def test_sync_context_manager_required(sync_client: SyncClaudeWorkerClient) -> None:
    """Calling methods outside the context manager raises RuntimeError."""
    with pytest.raises(RuntimeError, match="context manager"):
        sync_client.health()
