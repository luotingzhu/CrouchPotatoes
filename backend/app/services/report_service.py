"""核心服务二：生成定制化 AI 报告并向量化存储。

流程
----
    generate_for_user(user_id)
        │ 1) 读取用户上下文：角色(Manager/Sales/Tech) + 订阅模块(新闻/政治/商业/金融)
        ▼
    2) 按模块 + 时间窗拉取 raw_data（近 N 小时）
        │
        ▼
    3) LLM 按段合成：摘要(Summary) / 经济分析(Economic) / 政策(Policy)
        │   提示词随角色切换侧重点
        ▼
    4) 事务落库：reports + report_sections + report_sources(溯源 M:N)
        │
        ▼
    5) 报告段落切块 + embedding → 写入向量库（RAG 检索用）

要点
----
  * 报告可溯源：每个 section 通过 report_sources 关联到原始 raw_data；
  * 向量化放在事务提交之后，避免外部 API 调用拖长数据库事务。
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.events import bus
from app.db.models import (
    Module,
    RawData,
    Report,
    ReportSection,
    ReportSource,
    Role,
    UserModulePref,
    UserRole,
)
from app.db.session import AsyncSessionLocal
from app.services.ai_client import chat_complete, embed_texts
from app.services.text_utils import chunk_text
from app.services.vector_store import VectorItem, get_vector_store

logger = logging.getLogger(__name__)
settings = get_settings()

# 报告段落类型 → 生成侧重
SECTION_TYPES = ("summary", "economic", "policy")

# 角色 → 分析侧重（影响提示词口吻与结论导向）
ROLE_FOCUS = {
    "Manager": "面向管理者的战略与决策视角，突出风险、机会与可执行建议",
    "Sales": "面向销售的商机视角，突出客户需求、竞品动态与可跟进线索",
    "Tech": "面向技术的工程视角，突出技术趋势、标准变化与实现要点",
}

SYSTEM_PROMPT = (
    "你是 Domel 电机市场情报分析助手。你只会依据给定的原始资料作答，"
    "不编造事实；如资料不足须明确说明。输出使用结构化、要点化的中文。"
)


@dataclass
class UserContext:
    user_id: uuid.UUID
    roles: list[str]
    module_ids: list[int]
    module_names: list[str]


class ReportService:
    """报告生成 + 向量化。"""

    async def generate_for_user(self, user_id: uuid.UUID) -> Optional[uuid.UUID]:
        """为单个用户生成一份定制报告，返回 report_id。"""
        async with AsyncSessionLocal() as session:
            # 1) 用户上下文（角色 + 订阅模块）
            ctx = await self._load_user_context(session, user_id)
            if not ctx.module_ids:
                logger.info("用户 %s 未订阅任何模块，跳过报告生成", user_id)
                return None

            # 2) 拉取时间窗内的原始数据
            docs = await self._select_recent_docs(session, ctx.module_ids)
            if not docs:
                logger.info("用户 %s 在时间窗内无新数据", user_id)
                return None
            await bus.publish(
                "report",
                f"为 {user_id} 聚合到 {len(docs)} 条原始数据，开始生成报告",
                modules=ctx.module_names,
            )

            # 3) LLM 分段合成
            sections = await self._synthesize(ctx, docs)

            # 4) 落库（报告 + 段落 + 溯源）
            report = await self._persist(session, ctx, docs, sections)
            await session.commit()
            report_id = report.report_id
            # 拿到已生成的主键，供索引使用
            section_rows = [(s.section_id, s.content) for s in report.sections]

        await bus.publish(
            "report", f"报告已落库（含 {len(section_rows)} 个段落、{len(docs)} 条溯源）",
            report_id=str(report_id),
        )

        # 5) 报告段落向量化（事务外执行；失败不影响报告本身）
        try:
            await self._index_sections(section_rows)
            await bus.publish(
                "report", "报告段落已向量化，可用于对话检索", report_id=str(report_id)
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("报告向量化失败 report_id=%s", report_id)
            await bus.publish(
                "embed",
                f"报告向量化失败（报告已生成，可稍后重试）：{exc}",
                level="error",
                report_id=str(report_id),
            )
        logger.info("用户 %s 报告生成完成: %s", user_id, report_id)
        return report_id

    # =====================================================================
    # 步骤 1：加载用户上下文
    # =====================================================================
    async def _load_user_context(self, session: AsyncSession, user_id: uuid.UUID) -> UserContext:
        # 角色
        role_stmt = (
            select(Role.role_name)
            .join(UserRole, UserRole.role_id == Role.role_id)
            .where(UserRole.user_id == user_id)
        )
        roles = list((await session.execute(role_stmt)).scalars().all())

        # 订阅且启用的模块
        module_stmt = (
            select(Module.module_id, Module.module_name)
            .join(UserModulePref, UserModulePref.module_id == Module.module_id)
            .where(UserModulePref.user_id == user_id, UserModulePref.is_enabled.is_(True))
        )
        rows = (await session.execute(module_stmt)).all()
        return UserContext(
            user_id=user_id,
            roles=roles,
            module_ids=[r.module_id for r in rows],
            module_names=[r.module_name for r in rows],
        )

    # =====================================================================
    # 步骤 2：拉取原始数据（时间窗 + 模块过滤）
    # =====================================================================
    async def _select_recent_docs(
        self, session: AsyncSession, module_ids: list[int], limit: int = 50
    ) -> list[RawData]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=settings.report_lookback_hours)
        stmt = (
            select(RawData)
            .where(
                RawData.module_id.in_(module_ids),
                RawData.publish_time >= cutoff,       # 命中 ix_raw_data_publish_time（含分区裁剪）
            )
            .order_by(RawData.publish_time.desc())
            .limit(limit)
        )
        return list((await session.execute(stmt)).scalars().all())

    # =====================================================================
    # 步骤 3：LLM 分段合成
    # =====================================================================
    async def _synthesize(
        self, ctx: UserContext, docs: list[RawData]
    ) -> dict[str, str]:
        # 组装资料上下文：编号便于报告溯源引用
        corpus = "\n\n".join(
            f"[{i + 1}] {d.title}\n{(d.content or '')[:800]}" for i, d in enumerate(docs)
        )
        focus = "；".join(ROLE_FOCUS[r] for r in ctx.roles if r in ROLE_FOCUS) or "通用市场分析视角"
        modules = "、".join(ctx.module_names)

        section_instructions = {
            "summary": "输出【高亮摘要】：用条列提炼最重要的 5-8 条情报，每条注明引用编号，如 [3]。",
            "economic": "输出【经济分析】：分析供需、价格、产能与竞争格局，给出趋势判断与依据。",
            "policy": "输出【国家政策】：梳理可能影响电机市场的政策/法规/补贴，说明其潜在影响。",
        }

        sections: dict[str, str] = {}
        for section_type in SECTION_TYPES:
            user_prompt = (
                f"用户角色侧重：{focus}\n"
                f"关注模块：{modules}\n\n"
                f"{section_instructions[section_type]}\n\n"
                f"以下是原始资料：\n{corpus}"
            )
            sections[section_type] = await chat_complete(SYSTEM_PROMPT, user_prompt)
        return sections

    # =====================================================================
    # 步骤 4：事务落库（报告 + 段落 + 溯源）
    # =====================================================================
    async def _persist(
        self,
        session: AsyncSession,
        ctx: UserContext,
        docs: list[RawData],
        sections: dict[str, str],
    ) -> Report:
        report = Report(
            user_id=ctx.user_id,
            title=f"{datetime.now(timezone.utc):%Y-%m-%d} 定制市场情报报告",
            status="generated",
        )
        report.sections = [
            ReportSection(section_type=stype, content=content)
            for stype, content in sections.items()
        ]
        # 溯源：把本次使用的原始数据全部关联到报告（M:N）
        report.sources = [
            ReportSource(
                data_id=d.data_id,
                data_publish_time=d.publish_time,   # 复合外键需带分区键
                relevance=1.0,
                snippet=(d.title or "")[:200],
            )
            for d in docs
        ]
        session.add(report)
        await session.flush()          # 生成 report_id / section_id
        return report

    # =====================================================================
    # 步骤 5：报告段落切块 + 向量化
    # =====================================================================
    async def _index_sections(self, section_rows: list[tuple[uuid.UUID, str]]) -> None:
        items: list[VectorItem] = []
        for section_id, content in section_rows:
            chunks = chunk_text(content)
            if not chunks:
                continue
            vectors = await embed_texts(chunks)
            items.extend(
                VectorItem(
                    owner_type="report_section",
                    owner_id=section_id,
                    chunk_index=i,
                    chunk_text=text,
                    embedding=vec,
                )
                for i, (text, vec) in enumerate(zip(chunks, vectors))
            )
        if not items:
            return
        async with AsyncSessionLocal() as session:
            store = get_vector_store(session)
            await store.upsert(items)
            await session.commit()
