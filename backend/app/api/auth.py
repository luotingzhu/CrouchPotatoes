"""认证与用户信息路由。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, load_user_modules, load_user_roles
from app.core.events import bus
from app.core.security import create_session, drop_session, hash_password, verify_password
from app.db.models import Module, Role, User, UserModulePref, UserRole
from app.db.session import get_session
from app.schemas import AuthOut, LoginIn, ModuleOut, RegisterIn, RoleOut, UserOut

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.get("/meta", response_model=dict)
async def get_meta(session: AsyncSession = Depends(get_session)) -> dict:
    """返回可选角色与模块字典，供注册/配置页渲染。"""
    roles = (await session.execute(select(Role).order_by(Role.role_id))).scalars().all()
    modules = (await session.execute(select(Module).order_by(Module.module_id))).scalars().all()
    return {
        "roles": [RoleOut(role_id=r.role_id, role_name=r.role_name) for r in roles],
        "modules": [
            ModuleOut(module_id=m.module_id, module_code=m.module_code, module_name=m.module_name)
            for m in modules
        ],
    }


@router.post("/register", response_model=AuthOut)
async def register(payload: RegisterIn, session: AsyncSession = Depends(get_session)) -> AuthOut:
    user = User(
        username=payload.username,
        email=payload.email,
        password_hash=hash_password(payload.password),
    )
    session.add(user)
    try:
        await session.flush()
    except IntegrityError:
        await session.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, "用户名或邮箱已存在")

    # 绑定角色
    if payload.role_names:
        role_ids = (
            await session.execute(select(Role.role_id, Role.role_name))
        ).all()
        mapping = {name: rid for rid, name in role_ids}
        for name in payload.role_names:
            if name in mapping:
                session.add(UserRole(user_id=user.user_id, role_id=mapping[name]))

    # 绑定订阅模块
    if payload.module_codes:
        mods = (await session.execute(select(Module.module_id, Module.module_code))).all()
        mmap = {code: mid for mid, code in mods}
        for code in payload.module_codes:
            if code in mmap:
                session.add(UserModulePref(user_id=user.user_id, module_id=mmap[code]))

    await session.commit()
    token = await create_session(user.user_id)
    await bus.publish("system", f"新用户注册：{user.username}")
    return AuthOut(
        token=token,
        user=UserOut(
            user_id=user.user_id,
            username=user.username,
            email=user.email,
            roles=payload.role_names,
            modules=payload.module_codes,
        ),
    )


@router.post("/login", response_model=AuthOut)
async def login(payload: LoginIn, session: AsyncSession = Depends(get_session)) -> AuthOut:
    user = (
        await session.execute(select(User).where(User.username == payload.username))
    ).scalar_one_or_none()
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "用户名或密码错误")

    token = await create_session(user.user_id)
    roles = await load_user_roles(session, user.user_id)
    modules = await load_user_modules(session, user.user_id)
    await bus.publish("system", f"用户登录：{user.username}")
    return AuthOut(
        token=token,
        user=UserOut(
            user_id=user.user_id,
            username=user.username,
            email=user.email,
            roles=roles,
            modules=modules,
        ),
    )


@router.post("/logout")
async def logout(authorization: str | None = Header(default=None)) -> dict:
    if authorization and authorization.lower().startswith("bearer "):
        await drop_session(authorization.split(" ", 1)[1].strip())
    return {"ok": True}


@router.get("/me", response_model=UserOut)
async def me(
    user: User = Depends(get_current_user), session: AsyncSession = Depends(get_session)
) -> UserOut:
    return UserOut(
        user_id=user.user_id,
        username=user.username,
        email=user.email,
        roles=await load_user_roles(session, user.user_id),
        modules=await load_user_modules(session, user.user_id),
    )
