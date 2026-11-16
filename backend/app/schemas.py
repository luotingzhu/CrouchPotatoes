"""API 请求/响应模型（Pydantic）。集中定义，前后端字段一一对应。"""
from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

# 邮箱格式校验（不额外引入 email-validator 依赖），规则：
#   * 总长度 6~254，本地部分不超过 64
#   * 本地部分允许常见字符，但不能以点开头/结尾，也不能出现连续点
#   * 域名按标签校验（标签不能以连字符开头/结尾），顶级域为 2~63 个字母
_EMAIL_RE = re.compile(
    r"^(?=.{6,254}$)(?=.{1,64}@)"
    r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~.-]+@"
    r"(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"[A-Za-z]{2,63}$"
)

_EMAIL_HINT = "邮箱格式不正确，请填写如 name@example.com"


def is_valid_email(value: str) -> bool:
    """判断邮箱是否符合标准格式。"""
    if not _EMAIL_RE.match(value):
        return False
    local = value.rsplit("@", 1)[0]
    if local.startswith(".") or local.endswith(".") or ".." in local:
        return False
    return True


# ---------- 认证 ----------
class RegisterIn(BaseModel):
    username: str = Field(min_length=2, max_length=64)
    email: str = Field(max_length=254)      # 长度下限与格式统一交给 is_valid_email 判断
    password: str = Field(min_length=6, max_length=128)
    role_names: list[str] = Field(default_factory=list)      # Manager / Sales / Tech
    module_codes: list[str] = Field(default_factory=list)    # news / politics / business / finance

    @field_validator("username")
    @classmethod
    def _strip_username(cls, v: str) -> str:
        """去掉首尾空白，避免「 abc 」这类输入通过校验。"""
        return v.strip()

    @field_validator("email")
    @classmethod
    def _check_email(cls, v: str) -> str:
        v = v.strip().lower()          # 统一小写，避免仅大小写不同的重复账号
        if not is_valid_email(v):
            raise ValueError(_EMAIL_HINT)
        return v


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


class TranslateIn(BaseModel):
    data_ids: list[uuid.UUID] = Field(default_factory=list)
    lang: str = "en"


class TranslatedItem(BaseModel):
    data_id: uuid.UUID
    title: str


class TranslateOut(BaseModel):
    items: list[TranslatedItem] = Field(default_factory=list)


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
    # 数据新鲜度：素材全部早于时间窗时置为 True，前端据此提示“仅有旧数据”
    data_stale: bool = False
    latest_source_time: Optional[datetime] = None
    lookback_hours: int = 72
    # 便于前端解释：哪些订阅模块没有新数据、哪些模块当前有新数据可订阅
    stale_module_codes: list[str] = Field(default_factory=list)
    suggest_module_codes: list[str] = Field(default_factory=list)


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
    lang: str = Field(default="zh", pattern="^(zh|en)$")   # 回答语言，与前端界面语言联动


class AskOut(BaseModel):
    session_id: uuid.UUID
    answer: str
    references: list[str] = Field(default_factory=list)
    cached: bool = False


# ---------- 系统状态（后端可视化） ----------
class TableStat(BaseModel):
    name: str
    count: int


class AdminUnlockIn(BaseModel):
    password: str = Field(min_length=1, max_length=128)


class AdminStatusOut(BaseModel):
    enabled: bool


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
