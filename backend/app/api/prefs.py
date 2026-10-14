"""个性化配置：用户角色与订阅模块。"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, load_user_modules, load_user_roles
from app.core.events import bus
from app.db.models import Module, Role, User, UserModulePref, UserRole
from app.db.session import get_session
from app.schemas import PrefsIn, UserOut

router = APIRouter(prefix="/api/prefs", tags=["prefs"])


@router.get("", response_model=UserOut)
async def get_prefs(
    user: User = Depends(get_current_user), session: AsyncSession = Depends(get_session)
) -> UserOut:
    return UserOut(
        user_id=user.user_id,
        username=user.username,
        email=user.email,
        roles=await load_user_roles(session, user.user_id),
        modules=await load_user_modules(session, user.user_id),
    )


@router.put("", response_model=UserOut)
async def update_prefs(
    payload: PrefsIn,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> UserOut:
    """整表覆盖式更新（先清空再写入），实现简单且语义清晰。"""
    # 角色
    await session.execute(delete(UserRole).where(UserRole.user_id == user.user_id))
    if payload.role_names:
        rows = (await session.execute(select(Role.role_id, Role.role_name))).all()
        mapping = {name: rid for rid, name in rows}
        for name in payload.role_names:
            if name in mapping:
                session.add(UserRole(user_id=user.user_id, role_id=mapping[name]))

    # 模块
    await session.execute(
        delete(UserModulePref).where(UserModulePref.user_id == user.user_id)
    )
    if payload.module_codes:
        rows = (await session.execute(select(Module.module_id, Module.module_code))).all()
        mmap = {code: mid for mid, code in rows}
        for code in payload.module_codes:
            if code in mmap:
                session.add(UserModulePref(user_id=user.user_id, module_id=mmap[code]))

    await session.commit()
    await bus.publish(
        "system",
        f"用户 {user.username} 更新偏好：角色={payload.role_names} 模块={payload.module_codes}",
    )
    return UserOut(
        user_id=user.user_id,
        username=user.username,
        email=user.email,
        roles=payload.role_names,
        modules=payload.module_codes,
    )
