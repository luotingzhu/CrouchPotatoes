"""AI 对话路由：会话管理与 RAG 问答。"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.cache import redis_client
from app.db.models import ChatMessage, ChatSession, User
from app.db.session import get_session
from app.schemas import AskIn, AskOut, ChatMessageOut, ChatSessionOut
from app.services.rag_service import RagService

router = APIRouter(prefix="/api/chat", tags=["chat"])


@router.get("/sessions", response_model=list[ChatSessionOut])
async def list_sessions(
    user: User = Depends(get_current_user), session: AsyncSession = Depends(get_session)
) -> list[ChatSessionOut]:
    rows = (
        await session.execute(
            select(ChatSession)
            .where(ChatSession.user_id == user.user_id)
            .order_by(ChatSession.last_active_at.desc())
            .limit(50)
        )
    ).scalars().all()
    return [
        ChatSessionOut(
            session_id=s.session_id,
            title=s.title,
            created_at=s.created_at,
            last_active_at=s.last_active_at,
        )
        for s in rows
    ]


@router.post("/sessions", response_model=ChatSessionOut)
async def create_session(
    user: User = Depends(get_current_user), session: AsyncSession = Depends(get_session)
) -> ChatSessionOut:
    obj = ChatSession(user_id=user.user_id, title="新会话")
    session.add(obj)
    await session.commit()
    return ChatSessionOut(
        session_id=obj.session_id,
        title=obj.title,
        created_at=obj.created_at,
        last_active_at=obj.last_active_at,
    )


@router.get("/sessions/{session_id}/messages", response_model=list[ChatMessageOut])
async def list_messages(
    session_id: uuid.UUID,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[ChatMessageOut]:
    # 校验归属，避免越权查看他人会话
    owner = await session.get(ChatSession, session_id)
    if owner is None or owner.user_id != user.user_id:
        raise HTTPException(404, "会话不存在")

    rows = (
        await session.execute(
            select(ChatMessage)
            .where(ChatMessage.session_id == session_id)
            .order_by(ChatMessage.created_at)
        )
    ).scalars().all()
    return [
        ChatMessageOut(
            message_id=m.message_id,
            sender_type=m.sender_type,
            content=m.content,
            created_at=m.created_at,
        )
        for m in rows
    ]


@router.delete("/sessions/{session_id}")
async def delete_session(
    session_id: uuid.UUID,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """删除一个对话：消息记录、会话本身，以及 Redis 里的上下文与问答缓存。"""
    owner = await session.get(ChatSession, session_id)
    if owner is None or owner.user_id != user.user_id:
        raise HTTPException(404, "会话不存在")

    await session.execute(
        delete(ChatMessage).where(ChatMessage.session_id == session_id)
    )
    await session.delete(owner)
    await session.commit()

    # Redis 里还留着这轮的短期上下文和问答缓存，不清掉会出现「删了还能问出旧答案」
    await redis_client.delete(f"chat:ctx:{session_id}")
    async for key in redis_client.scan_iter(f"chat:a:{session_id}:*"):
        await redis_client.delete(key)

    return {"ok": True, "deleted": str(session_id)}


@router.post("/ask", response_model=AskOut)
async def ask(
    payload: AskIn,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> AskOut:
    """基于报告与原始数据知识的多轮问答（RAG + Redis 缓存优先）。"""
    owner = await session.get(ChatSession, payload.session_id)
    if owner is None or owner.user_id != user.user_id:
        raise HTTPException(404, "会话不存在")

    result = await RagService().ask(
        user.user_id, payload.session_id, payload.question, lang=payload.lang
    )
    return AskOut(
        session_id=payload.session_id,
        answer=result["answer"],
        references=result.get("references", []),
        cached=result.get("cached", False),
    )
