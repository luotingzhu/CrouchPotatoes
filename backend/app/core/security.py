"""认证工具：密码哈希（PBKDF2，无需额外依赖） + Redis 会话令牌。

说明：为精简依赖，密码哈希使用标准库 hashlib.pbkdf2_hmac（等价于 passlib 的效果），
      会话使用 Redis 中的不透明令牌（opaque token），前端放 localStorage 携带。
"""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import uuid
from typing import Optional

from app.core.cache import redis_client

_ITERATIONS = 200_000
_SESSION_TTL = 7 * 24 * 3600   # 会话有效期 7 天


def hash_password(password: str) -> str:
    """生成 pbkdf2_sha256 哈希，格式：algo$iterations$salt$digest。"""
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _ITERATIONS)
    return f"pbkdf2_sha256${_ITERATIONS}${salt.hex()}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    """校验密码（常数时间比较，防时序攻击）。"""
    try:
        _algo, iterations, salt_hex, digest_hex = stored.split("$")
        dk = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), int(iterations)
        )
        return hmac.compare_digest(dk.hex(), digest_hex)
    except (ValueError, AttributeError):
        return False


# ---------------------------------------------------------------------------
# 会话（存 Redis，便于多进程共享 + 天然支持过期）
# ---------------------------------------------------------------------------
async def create_session(user_id: uuid.UUID) -> str:
    token = secrets.token_urlsafe(32)
    await redis_client.set(f"sess:{token}", str(user_id), ex=_SESSION_TTL)
    return token


async def resolve_session(token: str) -> Optional[uuid.UUID]:
    raw = await redis_client.get(f"sess:{token}")
    return uuid.UUID(raw) if raw else None


async def drop_session(token: str) -> None:
    await redis_client.delete(f"sess:{token}")
