"""API 请求/响应模型（Pydantic）。集中定义，前后端字段一一对应。"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


# ---------- 认证 ----------
class RegisterIn(BaseModel):
    username: str = Field(min_length=2, max_length=64)
    email: str = Field(max_length=255)
    password: str = Field(min_length=6, max_length=128)
    role_names: list[str] = Field(default_factory=list)      # Manager / Sales / Tech
    module_codes: list[str] = Field(default_factory=list)    # news / politics / business / finance


class LoginIn(BaseModel):
    username: str
    password: str


class UserOut(BaseModel):
    user_id: uuid.UUID
    username: str
    email: str
    roles: list[str] = Field(default_factory=list)
    modules: list[str] = Field(default_factory=list)


class AuthOut(BaseModel):
    token: str
    user: UserOut


# ---------- 字典 ----------
class RoleOut(BaseModel):
    role_id: int
    role_name: str


class ModuleOut(BaseModel):
    module_id: int
    module_code: str
    module_name: str


# ---------- 个性化配置 ----------
class PrefsIn(BaseModel):
    role_names: list[str] = Field(default_factory=list)
    module_codes: list[str] = Field(default_factory=list)


# ---------- 数据源 ----------
class SourceIn(BaseModel):
    source_name: str = Field(max_length=128)
    url: str
    source_type: str = Field(pattern="^(crawler|api)$")


class SourceOut(BaseModel):
    source_id: int
    source_name: str
    url: str
    source_type: str
    status: int


class TriggerOut(BaseModel):
    source_id: int
    sent: int


class RawDataOut(BaseModel):
    data_id: uuid.UUID
    title: str
    module_id: int
    publish_time: datetime
    url: Optional[str] = None


# ---------- 报告 ----------
class SectionOut(BaseModel):
    section_id: uuid.UUID
    section_type: str
    content: str


class ReportSourceOut(BaseModel):
    data_id: uuid.UUID
    title: Optional[str] = None
    url: Optional[str] = None
    publish_time: Optional[datetime] = None


class ReportOut(BaseModel):
    report_id: uuid.UUID
    title: str
    status: str
    generated_time: datetime
    sections: list[SectionOut] = Field(default_factory=list)
    sources: list[ReportSourceOut] = Field(default_factory=list)


class ReportBrief(BaseModel):
    report_id: uuid.UUID
    title: str
    status: str
    generated_time: datetime


# ---------- 对话 ----------
class ChatSessionOut(BaseModel):
    session_id: uuid.UUID
    title: Optional[str] = None
    created_at: datetime
    last_active_at: datetime


class ChatMessageOut(BaseModel):
    message_id: uuid.UUID
    sender_type: str
    content: str
    created_at: datetime


class AskIn(BaseModel):
    session_id: uuid.UUID
    question: str = Field(min_length=1)


class AskOut(BaseModel):
    session_id: uuid.UUID
    answer: str
    references: list[str] = Field(default_factory=list)
    cached: bool = False


# ---------- 系统状态（后端可视化） ----------
class TableStat(BaseModel):
    name: str
    count: int


class SystemStatus(BaseModel):
    postgres_ok: bool
    redis_ok: bool
    vector_backend: str
    broker_backend: str
    tables: list[TableStat]
    partitions: list[str]
    embedding_count: int
    llm_model: str
    embedding_model: str
    recent_events: list[dict[str, Any]] = Field(default_factory=list)
