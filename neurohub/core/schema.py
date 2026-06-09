from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class MessageType(StrEnum):
    INPUT_TEXT = "INPUT_TEXT"
    COMMAND = "COMMAND"
    QUERY = "QUERY"
    SYSTEM_EVENT = "SYSTEM_EVENT"
    INTENT = "INTENT"
    RESPONSE = "RESPONSE"


class BusMessage(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: str = "1.0"
    ts: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    source: str
    target: str | None = None  # None = broadcast
    type: MessageType
    payload: dict[str, Any]
    reply_to: str | None = None  # id исходного сообщения
    session_id: str | None = None

    model_config = {"frozen": True}  # сообщения иммутабельны
