"""核心服务三（补充）：RAG 对话，体现「Redis 缓存优先」策略。

流程
----
    ask(user_id, session_id, question)
        │ 1) 查 Redis 缓存(answer)  ──命中──▶ 直接返回（缓存优先）
        ▼ 未命中
    2) 取 Redis 短期上下文（最近 N 轮对话）
        │
        ▼
    3) 问题向量化 → 向量检索 Top-K（报告段落 / 原始数据）
        │
        ▼
    4) 拼装 Prompt → LLM 生成答案（要求基于检索片段，附引用）
        │
        ▼
    5) 回写：chat_messages 落库 + Redis 上下文 + Redis 结果缓存

缓存失效策略：同一 session 内问答以 问题指纹 为 key 做 cache-aside；
             写入新消息时会刷新上下文 TTL，保证短期记忆新鲜。
"""
from __future__ import annotations

import hashlib
import logging
import uuid

from app.core.cache import (
    append_chat_context,
    get_cached_answer,
    get_chat_context,
    redis_client,
    set_cached_answer,
)
from app.core.config import get_settings
from app.core.events import bus
from app.db.models import ChatMessage
from app.db.session import AsyncSessionLocal
from app.services.ai_client import chat_complete, embed_texts
from app.services.vector_store import get_vector_store

logger = logging.getLogger(__name__)
settings = get_settings()

RAG_SYSTEM_PROMPT = (
    "你是 FinBrief 电机市场情报助手。请严格基于【检索到的资料】回答用户问题，"
    "并在结论后用 [编号] 标注出处；资料不足时明确说明，不要编造。"
)

RAG_SYSTEM_PROMPT_EN = (
    "You are the FinBrief motor market intelligence assistant. Answer strictly from the retrieved "
    "material, cite sources with [n] markers, and state clearly when evidence is insufficient. "
    "Never fabricate facts. Answer in English."
)


class RagService:
    """基于 RAG 的多轮对话。"""

    async def ask(
        self,
        user_id: uuid.UUID,
        session_id: uuid.UUID,
        question: str,
        lang: str = "zh",
    ) -> dict:
        lang = "en" if lang == "en" else "zh"
        # 1) 缓存优先（缓存键包含语言，避免中英文答案串用）
        cache_key = self._cache_key(session_id, question, lang)
        cached = await get_cached_answer(cache_key)
        if cached is not None:
            logger.debug("命中问答缓存: %s", cache_key)
            await bus.publish(
                "chat",
                "问答命中 Redis 缓存，直接返回",
                "Answer served from Redis cache",
                cached=True,
            )
            return {**cached, "cached": True}

        # 2) Redis 短期上下文（多轮对话记忆）
        history = await get_chat_context(str(session_id))

        # 3) 向量检索：优先报告段落，其次原始数据
        query_vec = (await embed_texts([question]))[0]
        async with AsyncSessionLocal() as session:
            store = get_vector_store(session)
            hits = await store.search(
                query_vec, top_k=6, owner_types=["report_section", "raw_data"]
            )
        await bus.publish(
            "chat",
            f"向量检索命中 {len(hits)} 个片段",
            f"Vector search matched {len(hits)} chunks",
            hits=len(hits),
        )

        # 4) 拼装 Prompt 并调用 LLM
        retrieved = "\n\n".join(
            f"[{i + 1}] ({h.owner_type}) {h.chunk_text}" for i, h in enumerate(hits)
        )
        history_text = "\n".join(f"{t['role']}: {t['content']}" for t in history[-6:])
        if lang == "en":
            user_prompt = (
                f"[Conversation history]\n{history_text or '(none)'}\n\n"
                f"[Retrieved material]\n{retrieved or '(none)'}\n\n"
                f"[User question]\n{question}\n\n"
                "Answer in English only, even if the retrieved material is in Chinese."
            )
            system_prompt = RAG_SYSTEM_PROMPT_EN
        else:
            user_prompt = (
                f"【历史对话】\n{history_text or '（无）'}\n\n"
                f"【检索到的资料】\n{retrieved or '（无）'}\n\n"
                f"【用户问题】\n{question}"
            )
            system_prompt = RAG_SYSTEM_PROMPT
        answer = await chat_complete(system_prompt, user_prompt)

        # 5) 回写：落库（长期）+ Redis（短期 + 缓存）
        result = {
            "session_id": str(session_id),
            "answer": answer,
            "references": [h.owner_id for h in hits],
            "cached": False,
        }
        await self._persist_messages(session_id, question, answer)
        await append_chat_context(str(session_id), {"role": "user", "content": question})
        await append_chat_context(str(session_id), {"role": "ai", "content": answer})
        await set_cached_answer(cache_key, result)
        await bus.publish(
            "chat",
            "已生成回答，写回 chat_messages 与 Redis 缓存",
            "Answer generated; saved to chat_messages and Redis cache",
            cached=False,
        )

        # 刷新会话活跃时间（供列表排序/冷热判定）
        await redis_client.expire(f"chat:ctx:{session_id}", settings.redis_chat_context_ttl)
        return result

    async def _persist_messages(
        self, session_id: uuid.UUID, question: str, answer: str
    ) -> None:
        """消息落库（chat_messages 按月分区）。"""
        async with AsyncSessionLocal() as session:
            session.add_all(
                [
                    ChatMessage(session_id=session_id, sender_type="user", content=question),
                    ChatMessage(
                        session_id=session_id,
                        sender_type="ai",
                        content=answer,
                        meta={"model": settings.llm_model},
                    ),
                ]
            )
            await session.commit()

    @staticmethod
    def _cache_key(session_id: uuid.UUID, question: str, lang: str = "zh") -> str:
        q_hash = hashlib.sha256(question.encode("utf-8")).hexdigest()[:16]
        return f"chat:a:{session_id}:{lang}:{q_hash}"
