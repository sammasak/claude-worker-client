from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel


class GoalStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"


class Goal(BaseModel):
    id: str
    goal: str
    status: GoalStatus
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None
    reviewed_at: str | None = None
    result: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in (GoalStatus.DONE, GoalStatus.FAILED)


class HealthResponse(BaseModel):
    status: str
    claude_running: bool
    pending_goals: int
    in_progress_goals: int


class StreamEvent(BaseModel):
    """A single event from the goal SSE stream."""

    event_type: str  # "hook" or "message"
    data: str

    @property
    def is_done(self) -> bool:
        return self.data == "[DONE]"

    @property
    def is_failed(self) -> bool:
        return self.data.startswith("[FAILED:")

    @property
    def failure_code(self) -> str | None:
        if self.is_failed:
            return self.data[len("[FAILED:") : -1]
        return None
