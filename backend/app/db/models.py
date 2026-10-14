"""SQLAlchemy 2.0 ORM 模型（异步）。

与 db/schema.sql 一一对应，覆盖：
  用户与权限 / 个性化配置 / 数据源与原始数据 / AI 报告 / AI 对话 / 向量存储

要点：
  * 主键使用 UUID（server_default=gen_random_uuid），如需雪花 BIGINT 替换类型即可；
  * raw_data、chat_messages 为分区表，主键必须包含分区键；
  * 长文本 TEXT、时间 TIMESTAMPTZ、结构化字段 JSONB、向量 pgvector.Vector；
  * metadata 是 SQLAlchemy 保留属性名，故 ORM 属性命名为 meta，映射到列 "metadata"。
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.config import get_settings
from app.db.base import Base

# 便捷别名
_tstz = DateTime(timezone=True)
_uuid = UUID(as_uuid=True)

# 向量维度必须与 pgvector 列定义、Embedding 模型三者一致，统一从配置读取
settings = get_settings()


# =============================================================================
# 用户与权限
# =============================================================================
class User(Base):
    __tablename__ = "users"

    user_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, primary_key=True, server_default=text("gen_random_uuid()")
    )
    username: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        _tstz, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # 关系：角色 / 模块偏好 / 报告 / 会话
    role_links: Mapped[list[UserRole]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    module_prefs: Mapped[list[UserModulePref]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    reports: Mapped[list[Report]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    chat_sessions: Mapped[list[ChatSession]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class Role(Base):
    __tablename__ = "roles"

    role_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    role_name: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)

    user_links: Mapped[list[UserRole]] = relationship(
        back_populates="role", cascade="all, delete-orphan"
    )


class UserRole(Base):
    """用户-角色关联表（M:N）。"""

    __tablename__ = "user_roles"

    user_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, ForeignKey("users.user_id", ondelete="CASCADE"), primary_key=True
    )
    role_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("roles.role_id", ondelete="CASCADE"), primary_key=True
    )
    granted_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())

    user: Mapped[User] = relationship(back_populates="role_links")
    role: Mapped[Role] = relationship(back_populates="user_links")


# =============================================================================
# 个性化配置
# =============================================================================
class Module(Base):
    __tablename__ = "modules"

    module_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    module_code: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    module_name: Mapped[str] = mapped_column(String(64), nullable=False)

    user_prefs: Mapped[list[UserModulePref]] = relationship(
        back_populates="module", cascade="all, delete-orphan"
    )
    raw_data: Mapped[list[RawData]] = relationship(back_populates="module")


class UserModulePref(Base):
    """用户订阅的模块（个性化核心，M:N）。"""

    __tablename__ = "user_module_prefs"

    user_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, ForeignKey("users.user_id", ondelete="CASCADE"), primary_key=True
    )
    module_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("modules.module_id", ondelete="CASCADE"), primary_key=True
    )
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())

    user: Mapped[User] = relationship(back_populates="module_prefs")
    module: Mapped[Module] = relationship(back_populates="user_prefs")


# =============================================================================
# 数据源与原始数据
# =============================================================================
class DataSource(Base):
    __tablename__ = "data_sources"
    __table_args__ = (
        CheckConstraint("source_type IN ('crawler', 'api')", name="source_type"),
    )

    source_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_name: Mapped[str] = mapped_column(String(128), nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    source_type: Mapped[str] = mapped_column(String(16), nullable=False)  # crawler / api
    status: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default=text("1"))
    crawl_cron: Mapped[Optional[str]] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        _tstz, nullable=False, server_default=func.now(), onupdate=func.now()
    )

    raw_data: Mapped[list[RawData]] = relationship(back_populates="source")


class RawData(Base):
    """原始数据（按 publish_time 月度分区）。

    注意：
      * 分区键 publish_time 必须包含在主键中；
      * 分区表上的唯一约束也必须包含分区键，因此去重逻辑放在服务层（content_hash 查询）。
    """

    __tablename__ = "raw_data"
    __table_args__ = (
        PrimaryKeyConstraint("data_id", "publish_time", name="pk_raw_data"),
        Index("ix_raw_data_publish_time", "publish_time"),
        Index("ix_raw_data_source_id", "source_id"),
        Index("ix_raw_data_module_id", "module_id"),
        Index("ix_raw_data_content_hash", "source_id", "content_hash"),
        {"postgresql_partition_by": "RANGE (publish_time)"},
    )

    data_id: Mapped[uuid.UUID] = mapped_column(_uuid, server_default=text("gen_random_uuid()"))
    source_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("data_sources.source_id", ondelete="RESTRICT"), nullable=False
    )
    module_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("modules.module_id", ondelete="RESTRICT"), nullable=False
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[Optional[str]] = mapped_column(Text)
    url: Mapped[Optional[str]] = mapped_column(Text)
    publish_time: Mapped[datetime] = mapped_column(_tstz, nullable=False)
    # meta -> 数据库列名 "metadata"，避免与 Declarative 的 Base.metadata 冲突
    meta: Mapped[dict] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    content_hash: Mapped[Optional[str]] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())

    source: Mapped[DataSource] = relationship(back_populates="raw_data")
    module: Mapped[Module] = relationship(back_populates="raw_data")


# =============================================================================
# AI 报告
# =============================================================================
class Report(Base):
    __tablename__ = "reports"
    __table_args__ = (
        CheckConstraint("status IN ('pending', 'generated', 'failed')", name="status"),
    )

    report_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'generated'"))
    generated_time: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())
    created_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())

    user: Mapped[User] = relationship(back_populates="reports")
    sections: Mapped[list[ReportSection]] = relationship(
        back_populates="report", cascade="all, delete-orphan"
    )
    sources: Mapped[list[ReportSource]] = relationship(
        back_populates="report", cascade="all, delete-orphan"
    )


class ReportSection(Base):
    __tablename__ = "report_sections"
    __table_args__ = (
        CheckConstraint(
            "section_type IN ('summary', 'economic', 'policy')", name="section_type"
        ),
    )

    section_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, primary_key=True, server_default=text("gen_random_uuid()")
    )
    report_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, ForeignKey("reports.report_id", ondelete="CASCADE"), nullable=False
    )
    section_type: Mapped[str] = mapped_column(String(32), nullable=False)  # summary/economic/policy
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())

    report: Mapped[Report] = relationship(back_populates="sections")


class ReportSource(Base):
    """报告-原始数据溯源关联表（M:N）。

    因 raw_data 为分区表、其主键为 (data_id, publish_time)，
    故此处复合外键同时携带 data_publish_time。
    """

    __tablename__ = "report_sources"
    __table_args__ = (
        ForeignKeyConstraint(
            ["data_id", "data_publish_time"],
            ["raw_data.data_id", "raw_data.publish_time"],
            ondelete="CASCADE",
            name="fk_report_sources_raw_data",
        ),
    )

    report_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, ForeignKey("reports.report_id", ondelete="CASCADE"), primary_key=True
    )
    data_id: Mapped[uuid.UUID] = mapped_column(_uuid, primary_key=True)
    data_publish_time: Mapped[datetime] = mapped_column(_tstz, nullable=False)
    relevance: Mapped[float] = mapped_column(Float, nullable=False, server_default=text("1.0"))
    snippet: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())

    report: Mapped[Report] = relationship(back_populates="sources")


# =============================================================================
# AI 对话
# =============================================================================
class ChatSession(Base):
    __tablename__ = "chat_sessions"

    session_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False
    )
    title: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())
    last_active_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())

    user: Mapped[User] = relationship(back_populates="chat_sessions")
    messages: Mapped[list[ChatMessage]] = relationship(
        back_populates="session", cascade="all, delete-orphan"
    )


class ChatMessage(Base):
    """对话消息（按 created_at 月度分区，主键须含分区键）。"""

    __tablename__ = "chat_messages"
    __table_args__ = (
        PrimaryKeyConstraint("message_id", "created_at", name="pk_chat_messages"),
        CheckConstraint("sender_type IN ('user', 'ai', 'system')", name="sender_type"),
        Index("ix_chat_messages_session", "session_id", "created_at"),
        {"postgresql_partition_by": "RANGE (created_at)"},
    )

    message_id: Mapped[uuid.UUID] = mapped_column(_uuid, server_default=text("gen_random_uuid()"))
    session_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, ForeignKey("chat_sessions.session_id", ondelete="CASCADE"), nullable=False
    )
    sender_type: Mapped[str] = mapped_column(String(8), nullable=False)  # user / ai / system
    content: Mapped[str] = mapped_column(Text, nullable=False)
    meta: Mapped[dict] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())

    session: Mapped[ChatSession] = relationship(back_populates="messages")


# =============================================================================
# 向量存储（pgvector 库内表）
# =============================================================================
class Embedding(Base):
    """文本切块向量表：raw_data / report_section / chat_message 的切块向量。"""

    __tablename__ = "embeddings"
    __table_args__ = (
        CheckConstraint(
            "owner_type IN ('raw_data', 'report_section', 'chat_message')", name="owner"
        ),
        Index("ix_embeddings_owner", "owner_type", "owner_id"),
    )

    embedding_id: Mapped[uuid.UUID] = mapped_column(
        _uuid, primary_key=True, server_default=text("gen_random_uuid()")
    )
    owner_type: Mapped[str] = mapped_column(String(24), nullable=False)
    owner_id: Mapped[uuid.UUID] = mapped_column(_uuid, nullable=False)
    chunk_index: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[list[float]] = mapped_column(
        Vector(settings.embedding_dim), nullable=False
    )
    model_name: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(_tstz, nullable=False, server_default=func.now())
