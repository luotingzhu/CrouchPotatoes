"""系统状态路由：把后端真实状态暴露给前端（「同步在后端」的可视化来源）。"""
from __future__ import annotations

import asyncio
import hmac
import json

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.cache import redis_client
from app.core.config import get_settings
from app.core.events import bus
from app.db.models import (
    ChatMessage,
    ChatSession,
    DataSource,
    Embedding,
    RawData,
    Report,
    ReportSection,
    User,
)
from app.db.session import get_session
from app.schemas import AdminStatusOut, AdminUnlockIn, SystemStatus, TableStat

router = APIRouter(prefix="/api/system", tags=["system"])
settings = get_settings()

_ADMIN_TTL = settings.admin_session_ttl


def _admin_key(user_id) -> str:
    """管理员解锁标记存放在 Redis，与用户绑定并带过期时间。"""
    return f"admin:{user_id}"


async def require_admin(user: User = Depends(get_current_user)) -> User:
    """要求当前用户已解锁管理员模式。"""
    if not await redis_client.get(_admin_key(user.user_id)):
        raise HTTPException(403, "需要管理员模式")
    return user


@router.get("/admin/status", response_model=AdminStatusOut)
async def admin_status(user: User = Depends(get_current_user)) -> AdminStatusOut:
    return AdminStatusOut(enabled=bool(await redis_client.get(_admin_key(user.user_id))))


@router.post("/admin/unlock", response_model=AdminStatusOut)
async def admin_unlock(
    payload: AdminUnlockIn, user: User = Depends(get_current_user)
) -> AdminStatusOut:
    # 常数时间比较，避免时序侧信道
    if not hmac.compare_digest(payload.password, settings.admin_password):
        raise HTTPException(403, "管理员密码错误")
    await redis_client.set(_admin_key(user.user_id), "1", ex=_ADMIN_TTL)
    await bus.publish(
        "system",
        f"用户 {user.username} 开启了管理员模式",
        f"User {user.username} enabled admin mode",
    )
    return AdminStatusOut(enabled=True)


@router.post("/admin/lock", response_model=AdminStatusOut)
async def admin_lock(user: User = Depends(get_current_user)) -> AdminStatusOut:
    await redis_client.delete(_admin_key(user.user_id))
    await bus.publish(
        "system",
        f"用户 {user.username} 关闭了管理员模式",
        f"User {user.username} disabled admin mode",
    )
    return AdminStatusOut(enabled=False)


@router.get("/status", response_model=SystemStatus)
async def status(
    session: AsyncSession = Depends(get_session),
    _admin: User = Depends(require_admin),
) -> SystemStatus:
    """采集数据库真实计数、分区列表、向量数、缓存与外部服务连通性。"""
    tables = [
        ("users", User),
        ("data_sources", DataSource),
        ("raw_data", RawData),
        ("reports", Report),
        ("report_sections", ReportSection),
        ("chat_sessions", ChatSession),
        ("chat_messages", ChatMessage),
        ("embeddings", Embedding),
    ]
    stats: list[TableStat] = []
    for name, model in tables:
        cnt = (
            await session.execute(select(func.count()).select_from(model))
        ).scalar_one()
        stats.append(TableStat(name=name, count=int(cnt)))

    # 分区列表：直接查 pg_inherits，展示「按时间分区」的真实结果
    part_rows = (
        await session.execute(
            text(
                """
                SELECT c.relname AS part_name
                FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                WHERE p.relname IN ('raw_data', 'chat_messages')
                ORDER BY c.relname
                """
            )
        )
    ).all()
    partitions = [r.part_name for r in part_rows]

    embedding_count = (
        await session.execute(select(func.count()).select_from(Embedding))
    ).scalar_one()

    # 连通性探针
    postgres_ok = True
    try:
        await session.execute(text("SELECT 1"))
    except Exception:
        postgres_ok = False

    redis_ok = True
    try:
        from app.core.cache import redis_client

        await redis_client.ping()
    except Exception:
        redis_ok = False

    return SystemStatus(
        postgres_ok=postgres_ok,
        redis_ok=redis_ok,
        vector_backend=settings.vector_backend,
        broker_backend=settings.broker_backend,
        tables=stats,
        partitions=partitions,
        embedding_count=int(embedding_count),
        llm_model=settings.llm_model,
        embedding_model=settings.embedding_model,
        recent_events=bus.history(30),
    )


@router.get("/events/recent")
async def recent_events(n: int = 20) -> list[dict]:
    """最近事件（供 Serverless 环境轮询；SSE 长连接在函数里会被超时断开）。"""
    return bus.history(min(max(n, 1), 100))


@router.get("/events")
async def events(request: Request) -> StreamingResponse:
    """SSE 事件流：实时推送「抓取 → 入库 → 向量化 → 报告」全过程。

    说明：EventSource 无法自定义请求头，故该端点不做令牌校验（仅演示用途）。
    """
    queue = bus.subscribe()

    async def generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"        # 心跳，保持连接
        finally:
            bus.unsubscribe(queue)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
