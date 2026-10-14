"""Redis 客户端：Session、Chat 短期上下文、问答缓存。

缓存策略：AI Chat 查询优先读缓存（cache-aside），未命中再走 RAG 检索 + LLM。
"""
import json
from typing import Any, Optional

import redis.asyncio as aioredis

from app.core.config import get_settings

settings = get_settings()

# 全局连接池（decode_responses=True，直接拿到 str）
redis_client: aioredis.Redis = aioredis.from_url(
    settings.redis_url, encoding="utf-8", decode_responses=True
)


# ---------- Chat 短期上下文（List 结构，只保留最近 N 轮） ----------
async def get_chat_context(session_id: str) -> list[dict[str, Any]]:
    key = f"chat:ctx:{session_id}"
    raw = await redis_client.lrange(key, 0, -1)
    return [json.loads(item) for item in raw]


async def append_chat_context(session_id: str, turn: dict[str, Any], keep: int = 10) -> None:
    key = f"chat:ctx:{session_id}"
    pipe = redis_client.pipeline()
    pipe.rpush(key, json.dumps(turn, ensure_ascii=False))
    pipe.ltrim(key, -keep, -1)                       # 仅保留最近 keep 轮
    pipe.expire(key, settings.redis_chat_context_ttl)
    await pipe.execute()


# ---------- 问答结果缓存（cache-aside） ----------
async def get_cached_answer(cache_key: str) -> Optional[dict[str, Any]]:
    raw = await redis_client.get(cache_key)
    return json.loads(raw) if raw else None


async def set_cached_answer(cache_key: str, value: dict[str, Any]) -> None:
    await redis_client.set(
        cache_key, json.dumps(value, ensure_ascii=False), ex=settings.redis_qa_cache_ttl
    )
