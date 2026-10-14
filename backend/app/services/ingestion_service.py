"""核心服务一：多源数据抓取入库（抓取 → Broker → raw_data → 向量化）。

数据流
------
    调度器/Airflow 触发 run_source(source_id)
        │  抓取(API/爬虫) → 清洗归一化
        ▼
    Broker.publish(topic=domel.raw_data)      ← 解耦 + 削峰（精简栈为内存队列，可切 Kafka）
        │
    consume_loop() 消费
        │  content_hash 去重
        ▼
    raw_data 入库（按月分区）──提交事务──▶ _embed_and_store() 切块 + Embedding
                                              ▼
                                        pgvector / Qdrant

设计取舍
--------
  * 抓取与入库解耦：抓取端只管投递 Broker，入库端按自身吞吐消费；
  * 先入库后向量化：外部 Embedding 调用不持有数据库事务；
  * 去重：分区表无法建全局唯一约束，用 (source_id, content_hash) 查询去重；
  * 每个阶段通过事件总线 bus 推送给前端，便于演示「后端同步」。
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.events import bus
from app.db.models import DataSource, RawData
from app.db.session import AsyncSessionLocal
from app.pipeline.broker import get_broker
from app.services.ai_client import embed_texts
from app.services.text_utils import chunk_text, sha256_hash
from app.services.vector_store import VectorItem, get_vector_store

logger = logging.getLogger(__name__)
settings = get_settings()

_HTTP_HEADERS = {"User-Agent": "DomelBot/1.0 (+market-intel)"}


class IngestionService:
    """多源数据抓取入库服务。"""

    # =====================================================================
    # 阶段 A：抓取并投递到 Broker
    # =====================================================================
    async def run_source(self, source_id: int) -> int:
        """抓取单个数据源并投递到 Broker，返回投递条数。"""
        async with AsyncSessionLocal() as session:
            source = await session.get(DataSource, source_id)
            if source is None or source.status != 1:
                logger.warning("数据源 %s 不存在或已停用，跳过", source_id)
                return 0

            await bus.publish("crawl", f"开始抓取数据源：{source.source_name}", source_id=source_id)
            if source.source_type == "api":
                docs = await self._fetch_api(source)
            else:
                docs = await self._crawl(source)

        broker = get_broker()
        sent = 0
        for doc in docs:
            doc["source_id"] = source_id
            await broker.publish(settings.kafka_topic_raw, doc)
            sent += 1
        await bus.publish(
            "crawl",
            f"数据源 [{source.source_name}] 抓取完成，投递 {sent} 条",
            source_id=source_id,
            count=sent,
        )
        return sent

    async def _fetch_api(self, source: DataSource) -> list[dict[str, Any]]:
        """API 数据源：请求 JSON 并归一化。"""
        async with httpx.AsyncClient(timeout=15, headers=_HTTP_HEADERS) as client:
            resp = await client.get(source.url)
            resp.raise_for_status()
            payload = resp.json()
        items = payload if isinstance(payload, list) else payload.get("items", [])
        return [self._normalize(source, item) for item in items]

    async def _crawl(self, source: DataSource) -> list[dict[str, Any]]:
        """爬虫数据源：拉取 HTML 并解析条目（按站点结构适配）。"""
        async with httpx.AsyncClient(timeout=15, headers=_HTTP_HEADERS) as client:
            resp = await client.get(source.url)
            resp.raise_for_status()
            html = resp.text
        soup = BeautifulSoup(html, "lxml")
        docs: list[dict[str, Any]] = []
        for node in soup.select("article, .news-item, li.item"):
            title = node.select_one("h1, h2, h3, .title")
            body = node.select_one("p, .summary, .content")
            docs.append(
                self._normalize(
                    source,
                    {
                        "title": title.get_text(strip=True) if title else "",
                        "content": body.get_text(strip=True) if body else "",
                        "url": source.url,
                    },
                )
            )
        return docs

    def _normalize(self, source: DataSource, raw: dict[str, Any]) -> dict[str, Any]:
        """把异构原始条目归一化为统一结构。

        module_id 通常由上游关键词/NLP 分类器给出；原始条目未提供时回退默认模块。
        """
        return {
            "source_id": source.source_id,
            "module_id": int(raw.get("module_id") or 1),
            "title": (raw.get("title") or "").strip(),
            "content": (raw.get("content") or raw.get("summary") or "").strip(),
            "url": raw.get("url") or source.url,
            "publish_time": raw.get("publish_time")
            or raw.get("published_at")
            or datetime.now(timezone.utc).isoformat(),
            "metadata": {
                "language": raw.get("language"),
                "author": raw.get("author"),
                "tags": raw.get("tags", []),
            },
        }

    # =====================================================================
    # 阶段 B：消费 Broker 消息并入库（应用启动时挂常驻任务）
    # =====================================================================
    async def consume_loop(self) -> None:
        """常驻消费循环。"""
        broker = get_broker()
        await bus.publish("system", "入库消费者已启动，等待消息…")
        async for payload in broker.subscribe(settings.kafka_topic_raw):
            try:
                await self.handle_message(payload)
            except Exception:
                logger.exception("消息处理失败: %s", payload.get("url"))
                await bus.publish(
                    "ingest", f"入库失败：{payload.get('title', '')[:40]}", level="error"
                )

    async def handle_message(self, payload: dict[str, Any]) -> Optional[uuid.UUID]:
        """单条消息入库 + 向量化，返回 data_id。"""
        content_hash = sha256_hash(f"{payload.get('title', '')}{payload.get('content', '')}")

        async with AsyncSessionLocal() as session:
            # 1) 去重：同一源 + 相同内容指纹视为重复
            if await self._is_duplicate(session, payload["source_id"], content_hash):
                await bus.publish(
                    "ingest", f"重复数据已跳过：{payload.get('title', '')[:40]}", level="muted"
                )
                return None

            # 2) 构建 ORM 对象并落库（publish_time 决定落入哪个分区）
            obj = RawData(
                source_id=payload["source_id"],
                module_id=payload["module_id"],
                title=payload["title"],
                content=payload.get("content"),
                url=payload.get("url"),
                publish_time=self._parse_ts(payload["publish_time"]),
                meta=payload.get("metadata", {}),
                content_hash=content_hash,
            )
            session.add(obj)
            await session.flush()            # 触发 INSERT，拿到 gen_random_uuid 主键
            data_id = obj.data_id
            await session.commit()           # 事务边界：先确保原始数据落库

        await bus.publish(
            "ingest",
            f"已入库 raw_data：{payload.get('title', '')[:40]}",
            data_id=str(data_id),
            module_id=payload["module_id"],
        )

        # 3) 事务提交后再做向量化（失败不影响已入库的数据，单独报错并提示可重试）
        try:
            await self._embed_and_store(data_id, payload)
        except Exception as exc:  # noqa: BLE001
            logger.exception("向量化失败 data_id=%s", data_id)
            await bus.publish(
                "embed",
                f"向量化失败（原始数据已入库，可稍后重试）：{exc}",
                level="error",
                data_id=str(data_id),
            )
        return data_id

    async def _embed_and_store(self, data_id: uuid.UUID, payload: dict[str, Any]) -> None:
        """对原始数据切块、向量化并写入向量库。"""
        full_text = f"{payload.get('title', '')}\n{payload.get('content', '')}".strip()
        chunks = chunk_text(full_text)
        if not chunks:
            return

        vectors = await embed_texts(chunks)
        items = [
            VectorItem(
                owner_type="raw_data",
                owner_id=data_id,
                chunk_index=i,
                chunk_text=text,
                embedding=vec,
            )
            for i, (text, vec) in enumerate(zip(chunks, vectors))
        ]

        async with AsyncSessionLocal() as session:
            store = get_vector_store(session)
            await store.upsert(items)
            await session.commit()

        await bus.publish(
            "embed",
            f"向量化完成：{len(items)} 个分块 → {settings.vector_backend}",
            data_id=str(data_id),
            chunks=len(items),
        )

    # =====================================================================
    # 工具方法
    # =====================================================================
    async def _is_duplicate(
        self, session: AsyncSession, source_id: int, content_hash: str
    ) -> bool:
        stmt = (
            select(RawData.data_id)
            .where(RawData.source_id == source_id, RawData.content_hash == content_hash)
            .limit(1)
        )
        return (await session.execute(stmt)).first() is not None

    @staticmethod
    def _parse_ts(value: Any) -> datetime:
        """解析时间字符串为带时区的 datetime。"""
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
