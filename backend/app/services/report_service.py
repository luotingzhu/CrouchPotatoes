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
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.config import get_settings
from app.core.events import bus
from app.db.models import (
    Embedding,
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
    "你是 finbrief 电机市场情报分析助手。你只会依据给定的原始资料作答，"
    "不编造事实；如资料不足须明确说明。输出使用结构化、要点化的中文。"
    "不要使用 Markdown 标记（不要 **加粗**、不要 # 标题、不要反引号），"
    "需要分层时用数字序号或「-」开头的短横线。"
)

SYSTEM_PROMPT_EN = (
    "You are the finbrief motor market intelligence analyst. Answer strictly from the provided "
    "source material and never fabricate facts; state clearly when evidence is insufficient. "
    "Output structured, bullet-pointed English. Do not use Markdown formatting — no **bold**, "
    "no # headings, no backticks; use numbers or a leading hyphen for lists."
)

# 角色 → 英文分析侧重
ROLE_FOCUS_EN = {
    "Manager": "a strategic, decision-oriented view for executives; highlight risks, opportunities and actionable recommendations",
    "Sales": "a sales-oriented view; highlight customer demand, competitor moves and leads worth following up",
    "Tech": "an engineering view; highlight technology trends, standards and implementation considerations",
}

# 模块中文名 → 英文名
MODULE_EN = {
    "新闻": "News",
    "政治": "Politics",
    "商业统计": "Business Stats",
    "金融": "Finance",
}

SECTION_INSTRUCTIONS_EN = {
    "summary": "Output HIGHLIGHTS: 5-8 bullets of the most important intelligence, each with citation numbers such as [3].",
    "economic": "Output ECONOMIC ANALYSIS: supply/demand, prices, capacity and competition, with trend judgments and evidence.",
    "policy": "Output GOVERNMENT POLICY: policies, regulations and subsidies that may affect the motor market, plus their implications.",
}


@dataclass
class UserContext:
    user_id: uuid.UUID
    roles: list[str]
    module_ids: list[int]
    module_names: list[str]


class ReportGenerationError(Exception):
    """报告无法生成的业务原因（由 API 层转成 400 返回前端）。"""


class ReportService:
    """报告生成 + 向量化。"""

    async def generate_for_user(
        self, user_id: uuid.UUID, lang: str = "zh"
    ) -> Optional[uuid.UUID]:
        """为单个用户生成一份定制报告，返回 report_id。

        lang 决定报告语言（zh / en）：英文模式下即使原始资料是中文，
        也会要求模型用英文撰写。
        """
        lang = "en" if lang == "en" else "zh"
        async with AsyncSessionLocal() as session:
            # 1) 用户上下文（角色 + 订阅模块）
            ctx = await self._load_user_context(session, user_id)
            if not ctx.module_ids:
                raise ReportGenerationError("尚未订阅任何情报模块，请先在「个性化配置」中选择模块")

            # 2) 拉取时间窗内的原始数据
            docs = await self._select_recent_docs(session, ctx.module_ids)
            if not docs:
                # 时间窗内没有新数据：回退到订阅模块下最新的历史数据，保证仍能生成
                docs = await self._select_latest_docs(session, ctx.module_ids)
                if not docs:
                    raise ReportGenerationError("你订阅的模块暂无任何数据，请先在「数据源与入库」中抓取数据")
                await bus.publish(
                    "report",
                    f"近 {settings.report_lookback_hours} 小时无新数据，改用最新 {len(docs)} 条历史数据生成报告",
                )
            await bus.publish(
                "report",
                f"为 {user_id} 聚合到 {len(docs)} 条原始数据，开始生成报告",
                modules=ctx.module_names,
            )

            # 3) LLM 分段合成（模型偶尔不按语言输出，这里补一道兜底）
            sections = await self._synthesize(ctx, docs, lang)
            sections = await self._ensure_language(sections, lang)

            # 4) 落库（报告 + 段落 + 溯源）
            report = await self._persist(session, ctx, docs, sections, lang)
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
    # 报告翻译：把已有报告（可能是中文）就地翻译成指定语言
    # =====================================================================
    async def translate_report(
        self, report_id: uuid.UUID, user_id: uuid.UUID, lang: str
    ) -> bool:
        target = "英文（English）" if lang == "en" else "中文"
        async with AsyncSessionLocal() as session:
            report = (
                await session.execute(
                    select(Report)
                    .where(Report.report_id == report_id, Report.user_id == user_id)
                    .options(selectinload(Report.sections))
                )
            ).scalar_one_or_none()
            if report is None:
                return False

            system = (
                "你是专业财经翻译。只输出译文，保持原有 Markdown 结构、数字、百分号"
                "与 [n] 引用标记不变，不要添加任何解释或前后缀。"
                f"目标语言：{target}。"
            )
            for section in report.sections:
                translated = await chat_complete(
                    system, f"请翻译以下内容：\n\n{section.content}"
                )
                if translated.strip():
                    section.content = translated.strip()

            title = await chat_complete(system, f"请翻译以下标题：\n{report.title}")
            if title.strip():
                report.title = title.strip().strip('"').strip()

            section_rows = [(s.section_id, s.content) for s in report.sections]
            section_ids = [s.section_id for s in report.sections]
            # 旧语言的向量已失效，先删除再重建
            await session.execute(
                delete(Embedding).where(
                    Embedding.owner_type == "report_section",
                    Embedding.owner_id.in_(section_ids),
                )
            )
            await session.commit()

        await self._index_sections(section_rows)
        await bus.publish("report", f"报告已翻译为{target}", report_id=str(report_id))
        return True

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
        """按模块分配配额取最近数据。

        之前是全局取最新 N 条，高产的新闻源会把宏观/商业统计这类低频但关键的
        数据挤出去；改为每个模块各取一份，再按时间合并。
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=settings.report_lookback_hours)
        quota = max(6, limit // max(1, len(module_ids)))

        docs: list[RawData] = []
        for mid in module_ids:
            stmt = (
                select(RawData)
                .where(
                    RawData.module_id == mid,
                    RawData.publish_time >= cutoff,
                )
                .order_by(RawData.publish_time.desc())
                .limit(quota)
            )
            docs.extend((await session.execute(stmt)).scalars().all())

        docs.sort(key=lambda d: d.publish_time, reverse=True)
        return docs[:limit]

    async def _select_latest_docs(
        self, session: AsyncSession, module_ids: list[int], limit: int = 50
    ) -> list[RawData]:
        """时间窗内无数据时的回退：取订阅模块下最新的历史数据（不限时间）。"""
        stmt = (
            select(RawData)
            .where(RawData.module_id.in_(module_ids))
            .order_by(RawData.publish_time.desc())
            .limit(limit)
        )
        return list((await session.execute(stmt)).scalars().all())

    # =====================================================================
    # 步骤 3：LLM 分段合成
    # =====================================================================
    async def _synthesize(
        self, ctx: UserContext, docs: list[RawData], lang: str = "zh"
    ) -> dict[str, str]:
        # 组装资料上下文：编号便于报告溯源引用
        corpus = "\n\n".join(
            f"[{i + 1}] {d.title}\n{(d.content or '')[:800]}" for i, d in enumerate(docs)
        )

        if lang == "en":
            focus = "; ".join(
                ROLE_FOCUS_EN[r] for r in ctx.roles if r in ROLE_FOCUS_EN
            ) or "a general market-analysis perspective"
            modules = ", ".join(MODULE_EN.get(n, n) for n in ctx.module_names)
            instructions = SECTION_INSTRUCTIONS_EN
            system_prompt = SYSTEM_PROMPT_EN
            header = (
                f"User role focus: {focus}\n"
                f"Modules of interest: {modules}\n\n"
            )
        else:
            focus = "；".join(
                ROLE_FOCUS[r] for r in ctx.roles if r in ROLE_FOCUS
            ) or "通用市场分析视角"
            modules = "、".join(ctx.module_names)
            instructions = {
                "summary": "输出【高亮摘要】：用条列提炼最重要的 5-8 条情报，每条注明引用编号，如 [3]。",
                "economic": "输出【经济分析】：分析供需、价格、产能与竞争格局，给出趋势判断与依据。",
                "policy": "输出【国家政策】：梳理可能影响电机市场的政策/法规/补贴，说明其潜在影响。",
            }
            system_prompt = SYSTEM_PROMPT
            header = f"用户角色侧重：{focus}\n关注模块：{modules}\n\n"

        sections: dict[str, str] = {}
        for section_type in SECTION_TYPES:
            if lang == "en":
                user_prompt = (
                    f"{header}{instructions[section_type]}\n\n"
                    "Write the entire answer in English, even if the source material is in Chinese. "
                    "Keep company names and technical terms in their original form when appropriate.\n\n"
                    f"Source material:\n{corpus}"
                )
            else:
                user_prompt = (
                    f"{header}{instructions[section_type]}\n\n"
                    f"以下是原始资料：\n{corpus}"
                )
            sections[section_type] = await chat_complete(system_prompt, user_prompt)
        return sections

    @staticmethod
    def _cjk_ratio(text: str) -> float:
        if not text:
            return 0.0
        return len(re.findall(r"[一-鿿]", text)) / len(text)

    async def _ensure_language(
        self, sections: dict[str, str], lang: str
    ) -> dict[str, str]:
        """确保正文语言与请求一致。

        模型即使被要求用英文，偶尔仍会整段输出中文（或反过来），
        这时在落库前自动翻一遍，用户就不必再点一次「翻译为英文」。
        """
        wrong = {
            k: v for k, v in sections.items()
            if (lang == "en" and self._cjk_ratio(v) > 0.15)
            or (lang == "zh" and self._cjk_ratio(v) < 0.05)
        }
        if not wrong:
            return sections

        target = "英文（English）" if lang == "en" else "中文"
        system = (
            "你是专业财经翻译。只输出译文，保持数字、百分号与 [n] 引用标记不变，"
            "不要添加任何解释或前后缀。"
            f"目标语言：{target}。"
        )
        out = dict(sections)
        for key, text in wrong.items():
            translated = await chat_complete(system, f"请翻译以下内容：\n\n{text}")
            if translated.strip():
                out[key] = translated.strip()
        await bus.publish("report", f"输出语言与请求不符，已自动翻译为{target}")
        return out

    # =====================================================================
    # 步骤 4：事务落库（报告 + 段落 + 溯源）
    # =====================================================================
    async def _persist(
        self,
        session: AsyncSession,
        ctx: UserContext,
        docs: list[RawData],
        sections: dict[str, str],
        lang: str = "zh",
    ) -> Report:
        stamp = f"{datetime.now(timezone.utc):%Y-%m-%d}"
        title = (
            f"{stamp} Custom Market Intelligence Report"
            if lang == "en"
            else f"{stamp} 定制市场情报报告"
        )
        report = Report(
            user_id=ctx.user_id,
            title=title,
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
