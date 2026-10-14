"""API 依赖：从请求头解析会话令牌，注入当前用户。"""
from __future__ import annotations

import uuid

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import resolve_session
from app.db.models import Module, Role, User, UserModulePref, UserRole
from app.db.session import get_session


async def get_current_user(
    authorization: str | None = Header(default=None),
    session: AsyncSession = Depends(get_session),
) -> User:
    """Bearer <token> → 用户；令牌存 Redis。"""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "缺少访问令牌")

    token = authorization.split(" ", 1)[1].strip()
    user_id = await resolve_session(token)
    if user_id is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "会话已失效，请重新登录")

    user = await session.get(User, user_id)
    if user is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "用户不存在")
    return user


async def load_user_roles(session: AsyncSession, user_id: uuid.UUID) -> list[str]:
    stmt = (
        select(Role.role_name)
        .join(UserRole, UserRole.role_id == Role.role_id)
        .where(UserRole.user_id == user_id)
    )
    return list((await session.execute(stmt)).scalars().all())


async def load_user_modules(session: AsyncSession, user_id: uuid.UUID) -> list[str]:
    stmt = (
        select(Module.module_code)
        .join(UserModulePref, UserModulePref.module_id == Module.module_id)
        .where(UserModulePref.user_id == user_id, UserModulePref.is_enabled.is_(True))
    )
    return list((await session.execute(stmt)).scalars().all())
