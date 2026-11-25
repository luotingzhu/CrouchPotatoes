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

from sqlalchemy import select

from app.core.cache import (
    append_chat_context,
    get_cached_answer,
    get_chat_context,
    redis_client,
    set_cached_answer,
)
from app.core.config import get_settings
from app.core.events import bus
from app.db.models import ChatMessage, RawData, ReportSection, ReportSource
from app.db.session import AsyncSessionLocal
from app.services.ai_client import chat_complete, embed_texts
from app.services.vector_store import get_vector_store

logger = logging.getLogger(__name__)
settings = get_settings()

RAG_SYSTEM_PROMPT = (
    "你是 FinBrief 电机市场情报助手。请严格基于【检索到的资料】回答用户问题，"
    "并在结论后用 [编号] 标注出处；资料不足时明确说明，不要编造。"
    "不要使用 Markdown 标记（不要 **加粗**、不要 # 标题），输出纯文本，"
    "需要分层时用数字序号或「-」开头的短横线。"
)

RAG_SYSTEM_PROMPT_EN = (
    "You are the FinBrief motor market intelligence assistant. Answer strictly from the retrieved "
    "material, cite sources with [n] markers, and state clearly when evidence is insufficient. "
    "Never fabricate facts. Answer in English. Do not use Markdown formatting — no **bold** and "
    "no # headings; use numbers or a leading hyphen for lists."
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
            "references": await self._build_references(hits),
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

    async def _build_references(self, hits) -> list[dict]:
        """把检索命中还原成可读引用：标题 + 原地址，序号与 prompt 里的 [n] 一一对应。"""
        raw_ids = [h.owner_id for h in hits if h.owner_type == "raw_data"]
        sec_ids = [h.owner_id for h in hits if h.owner_type == "report_section"]
        raw_map: dict = {}
        sec_map: dict = {}
        report_src_map: dict = {}

        async with AsyncSessionLocal() as session:
            if raw_ids:
                rows = (
                    await session.execute(
                        select(RawData.data_id, RawData.title, RawData.url, RawData.publish_time)
                        .where(RawData.data_id.in_(raw_ids))
                    )
                ).all()
                raw_map = {str(r[0]): r for r in rows}
            if sec_ids:
                rows = (
                    await session.execute(
                        select(
                            ReportSection.section_id,
                            ReportSection.section_type,
                            ReportSection.report_id,
                        ).where(ReportSection.section_id.in_(sec_ids))
                    )
                ).all()
                sec_map = {str(r[0]): r for r in rows}

                # 报告章节背后引用了哪些原始文章：章节 → report → report_sources → raw_data
                report_ids = {r[2] for r in rows}
                links = (
                    await session.execute(
                        select(ReportSource.report_id, ReportSource.data_id)
                        .where(ReportSource.report_id.in_(report_ids))
                    )
                ).all()
                data_ids = [r[1] for r in links]
                if data_ids:
                    drows = (
                        await session.execute(
                            select(RawData.data_id, RawData.title, RawData.url)
                            .where(RawData.data_id.in_(data_ids))
                        )
                    ).all()
                    dmap = {str(r[0]): (r[1], r[2]) for r in drows}
                    for rid, did in links:
                        pair = dmap.get(str(did))
                        if pair:
                            report_src_map.setdefault(str(rid), []).append(pair)

        refs: list[dict] = []
        for i, h in enumerate(hits, 1):
            snippet = " ".join((h.chunk_text or "").split())[:90]
            if h.owner_type == "raw_data":
                row = raw_map.get(str(h.owner_id))
                refs.append({
                    "index": i,
                    "owner_type": "raw_data",
                    "title": row[1] if row else None,
                    "url": row[2] if row else None,
                    "section_type": None,
                    "published_at": row[3].isoformat() if row and row[3] else None,
                    "snippet": snippet,
                    "sources": [],
                })
            elif h.owner_type == "report_section":
                row = sec_map.get(str(h.owner_id))
                behind = [
                    {"title": t, "url": u}
                    for t, u in report_src_map.get(str(row[2]), [])[:3]
                ] if row else []
                refs.append({
                    "index": i,
                    "owner_type": "report_section",
                    "title": None,
                    "url": None,
                    "section_type": row[1] if row else None,
                    "published_at": None,
                    "snippet": snippet,
                    "sources": behind,
                })
            else:
                refs.append({
                    "index": i,
                    "owner_type": h.owner_type,
                    "title": None,
                    "url": None,
                    "section_type": None,
                    "published_at": None,
                    "snippet": snippet,
                    "sources": [],
                })
        return refs

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
