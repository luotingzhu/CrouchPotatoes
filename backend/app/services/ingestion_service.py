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

import asyncio
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.events import bus
from app.core.source_names import source_name_en
from app.db.models import DataSource, RawData
from app.db.session import AsyncSessionLocal
from app.pipeline.broker import get_broker
from app.services.ai_client import embed_texts
from app.services.text_utils import chunk_text, sha256_hash
from app.services.vector_store import VectorItem, get_vector_store

logger = logging.getLogger(__name__)
settings = get_settings()

# 用浏览器 UA，避免常见站点的反爬拦截（原机器人 UA 会被大量站点直接拒绝）
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# 站点级抓取配置：列表页选择器 + 链接过滤 + 默认模块
# 说明：这些站点是 JS 渲染/结构各异，通用选择器抓不到，故按域名单独配置。
_SITE_PROFILES: dict[str, dict[str, Any]] = {
    "finance.eastmoney.com": {
        "list": "li > a", "href_has": "/a/", "module_id": 1,
    },
    "finance.sina.com.cn": {
        "list": "li.seaio_list > a", "href_has": "sina.com.cn", "module_id": 1,
    },
    "www.yicai.com": {
        "list": "div.m-con > a", "href_has": "yicai.com", "module_id": 4,
    },
    "finance.ifeng.com": {
        "list": 'div[class*="hotspotsList"] > a', "href_has": "ifeng.com", "module_id": 1,
    },
    # ---- 国际 / 全球市场来源 ----
    "www.cnbc.com": {
        "list": "div.LatestNews-headlineWrapper > a", "href_has": "cnbc.com", "module_id": 4,
    },
    "www.zhitongcaijing.com": {
        "list": "div.info-item-content-title > a",
        "href_has": "zhitongcaijing.com", "module_id": 4,
    },
    "www.gelonghui.com": {
        "list": "section.detail-right > a", "href_has": "gelonghui.com", "module_id": 4,
    },
    # ---- 政策 / 宏观统计（真实来源）----
    "www.news.cn": {
        "list": "li a", "href_has": "/politics/20", "module_id": 2,
    },
    "www.stats.gov.cn": {
        "list": "li a", "href_has": "/t20", "module_id": 3,
    },
}

# 正文容器候选（按优先级），取文本最长者
_BODY_SELECTORS = [
    "article", ".article", ".article-content", ".content", "#content",
    ".rich_media_content", ".post_content", ".main-content", ".text",
    ".ArticleBody-articleBody",   # CNBC
]

# 每个数据源单次最多抓取的正文篇数（控制耗时）
_MAX_ARTICLES_PER_SOURCE = 8

_DATE_RE = re.compile(
    r"(20\d{2})[-/年](\d{1,2})[-/月](\d{1,2})日?(?:[ T](\d{1,2}):(\d{2}))?"
)
_CST = timezone(timedelta(hours=8))


def _extract_body(soup: BeautifulSoup) -> str:
    """从文章页抽取正文：去掉导航/脚本后，取候选容器中最长的一段。"""
    for tag in soup(["script", "style", "nav", "header", "footer", "aside", "form"]):
        tag.decompose()
    best = ""
    for sel in _BODY_SELECTORS:
        for node in soup.select(sel):
            text = " ".join(p.get_text(" ", strip=True) for p in node.find_all("p"))
            if not text:
                text = node.get_text(" ", strip=True)
            if len(text) > len(best):
                best = text
    if len(best) < 80:
        best = " ".join(p.get_text(" ", strip=True) for p in soup.find_all("p"))
    return best.strip()


def _extract_publish_time(text: str) -> Optional[str]:
    """从正文开头尝试解析发布时间，失败则返回 None。"""
    match = _DATE_RE.search(text[:400])
    if not match:
        return None
    year, month, day, hour, minute = match.groups()
    try:
        dt = datetime(
            int(year), int(month), int(day),
            int(hour or 0), int(minute or 0), tzinfo=_CST,
        )
    except ValueError:
        return None
    return dt.isoformat()


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

            await bus.publish(
                "crawl",
                f"开始抓取数据源：{source.source_name}",
                f"Fetching source: {source_name_en(source.source_name)}",
                source_id=source_id,
            )
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
            f"Source [{source_name_en(source.source_name)}] fetched; {sent} items dispatched",
            source_id=source_id,
            count=sent,
        )
        return sent

    async def run_source_inline(self, source_id: int) -> int:
        """Serverless 模式：抓取后直接逐条入库并向量化，返回实际新增条数。

        没有常驻消费者可用，因此必须在本次请求内完成「抓取 → 入库 → 向量化」。
        """
        async with AsyncSessionLocal() as session:
            source = await session.get(DataSource, source_id)
            if source is None or source.status != 1:
                logger.warning("数据源 %s 不存在或已停用，跳过", source_id)
                return 0

            await bus.publish(
                "crawl",
                f"开始抓取数据源：{source.source_name}",
                f"Fetching source: {source_name_en(source.source_name)}",
                source_id=source_id,
            )
            docs = (
                await self._fetch_api(source)
                if source.source_type == "api"
                else await self._crawl(source)
            )

        stored = 0
        for doc in docs:
            doc["source_id"] = source_id
            try:
                if await self.handle_message(doc):
                    stored += 1
            except Exception:  # noqa: BLE001
                logger.exception("同步入库失败: %s", doc.get("url"))

        await bus.publish(
            "crawl",
            f"数据源 [{source.source_name}] 同步入库完成：抓取 {len(docs)} 条，新增 {stored} 条",
            f"Source [{source_name_en(source.source_name)}] ingested inline: {len(docs)} fetched, {stored} new",
            source_id=source_id,
            count=stored,
        )
        return stored

    async def _fetch_api(self, source: DataSource) -> list[dict[str, Any]]:
        """API 数据源：请求 JSON 并归一化。"""
        async with httpx.AsyncClient(timeout=15, headers=_HTTP_HEADERS) as client:
            resp = await client.get(source.url)
            resp.raise_for_status()
            payload = resp.json()
        items = payload if isinstance(payload, list) else payload.get("items", [])
        return [self._normalize(source, item) for item in items]

    async def _crawl(self, source: DataSource) -> list[dict[str, Any]]:
        """爬虫数据源：有站点配置时走「列表页 → 文章页」，否则走通用解析。"""
        host = urlparse(source.url).netloc.lower()
        profile = _SITE_PROFILES.get(host)

        async with httpx.AsyncClient(
            timeout=15, headers=_HTTP_HEADERS, follow_redirects=True
        ) as client:
            resp = await client.get(source.url)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            if profile:
                links = self._extract_links(soup, source.url, profile)
                docs = await self._fetch_articles(client, source, links, profile)
                if docs:
                    await bus.publish(
                        "crawl",
                        f"[{source.source_name}] 列表页 {len(links)} 条链接，成功抓取 {len(docs)} 篇正文",
                        f"[{source_name_en(source.source_name)}] {len(links)} links on list page, {len(docs)} articles fetched",
                        source_id=source.source_id,
                    )
                    return docs
                await bus.publish(
                    "crawl",
                    f"[{source.source_name}] 站点配置未命中，回退通用解析",
                    f"[{source_name_en(source.source_name)}] site profile missed; falling back to generic parsing",
                    level="warn",
                    source_id=source.source_id,
                )

        return self._parse_generic(source, soup)

    def _parse_generic(self, source: DataSource, soup: BeautifulSoup) -> list[dict[str, Any]]:
        """通用回退：适用于 article / .news-item / li.item 结构的页面。"""
        docs: list[dict[str, Any]] = []
        for node in soup.select("article, .news-item, li.item"):
            title = node.select_one("h1, h2, h3, .title")
            body = node.select_one("p, .summary, .content")
            link = node.select_one("a[href]")
            docs.append(
                self._normalize(
                    source,
                    {
                        "title": title.get_text(strip=True) if title else "",
                        "content": body.get_text(strip=True) if body else "",
                        "url": urljoin(source.url, link["href"]) if link else source.url,
                    },
                )
            )
        return docs

    def _extract_links(
        self, soup: BeautifulSoup, base_url: str, profile: dict[str, Any]
    ) -> list[tuple[str, str]]:
        """从列表页提取 (标题, 绝对链接)，按配置过滤并去重。"""
        links: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in soup.select(profile["list"]):
            title = a.get_text(strip=True)
            href = a.get("href") or ""
            if len(title) < 8 or not href:
                continue
            href = urljoin(base_url, href)
            if profile.get("href_has") and profile["href_has"] not in href:
                continue
            if href in seen:
                continue
            seen.add(href)
            links.append((title, href))
        return links[:_MAX_ARTICLES_PER_SOURCE]

    async def _fetch_articles(
        self,
        client: httpx.AsyncClient,
        source: DataSource,
        links: list[tuple[str, str]],
        profile: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """并发抓取文章正文；正文过短（广告/跳转页）则丢弃。"""
        sem = asyncio.Semaphore(5)

        async def one(title: str, href: str) -> Optional[dict[str, Any]]:
            async with sem:
                try:
                    resp = await client.get(href)
                    if resp.status_code != 200:
                        return None
                    body = _extract_body(BeautifulSoup(resp.text, "lxml"))
                    if len(body) < 120:
                        return None
                    return self._normalize(
                        source,
                        {
                            "title": title,
                            "content": body[:4000],
                            "url": href,
                            "publish_time": _extract_publish_time(body)
                            or datetime.now(timezone.utc).isoformat(),
                            "module_id": profile.get("module_id", 1),
                        },
                    )
                except Exception:  # noqa: BLE001
                    logger.debug("文章抓取失败: %s", href)
                    return None

        results = await asyncio.gather(*(one(t, h) for t, h in links))
        return [r for r in results if r]

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
        await bus.publish(
            "system",
            "入库消费者已启动，等待消息…",
            "Ingestion consumer started; waiting for messages...",
        )
        async for payload in broker.subscribe(settings.kafka_topic_raw):
            try:
                await self.handle_message(payload)
            except Exception:
                logger.exception("消息处理失败: %s", payload.get("url"))
                await bus.publish(
                    "ingest",
                    f"入库失败：{payload.get('title', '')[:40]}",
                    f"Ingestion failed: {payload.get('title', '')[:40]}",
                    level="error",
                )

    async def handle_message(self, payload: dict[str, Any]) -> Optional[uuid.UUID]:
        """单条消息入库 + 向量化，返回 data_id。"""
        content_hash = sha256_hash(f"{payload.get('title', '')}{payload.get('content', '')}")

        async with AsyncSessionLocal() as session:
            # 1) 去重：同一源 + 相同内容指纹视为重复
            if await self._is_duplicate(session, payload["source_id"], content_hash):
                await bus.publish(
                    "ingest",
                    f"重复数据已跳过：{payload.get('title', '')[:40]}",
                    f"Duplicate skipped: {payload.get('title', '')[:40]}",
                    level="muted",
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
            f"Stored in raw_data: {payload.get('title', '')[:40]}",
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
                f"Embedding failed (raw data stored; retry later): {exc}",
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
            f"Embedded {len(items)} chunks → {settings.vector_backend}",
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
