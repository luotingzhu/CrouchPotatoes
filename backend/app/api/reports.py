"""AI 报告路由：生成、列表、详情（含溯源）。"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import delete, func, select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.config import get_settings
from app.db.models import Embedding, Module, RawData, Report, ReportSection, User, UserModulePref
from app.db.session import get_session
from app.schemas import ReportBrief, ReportOut, ReportSourceOut, SectionOut
from app.services.report_service import ReportGenerationError, ReportService

router = APIRouter(prefix="/api/reports", tags=["reports"])
settings = get_settings()

# 报告段落的展示顺序（摘要 → 经济 → 政策），避免按字母序排导致阅读顺序错乱
_SECTION_ORDER = {"summary": 0, "economic": 1, "policy": 2}


@router.post("/generate", response_model=ReportOut)
async def generate_report(
    lang: str = "zh",
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> ReportOut:
    """按当前用户的角色 + 订阅模块生成定制报告；lang 控制报告语言（zh/en）。"""
    try:
        report_id = await ReportService().generate_for_user(user.user_id, lang=lang)
    except ReportGenerationError as exc:
        raise HTTPException(400, str(exc))
    if report_id is None:
        raise HTTPException(400, "报告生成失败，请稍后重试")
    return await _load_report_out(session, report_id)


@router.get("", response_model=list[ReportBrief])
async def list_reports(
    user: User = Depends(get_current_user), session: AsyncSession = Depends(get_session)
) -> list[ReportBrief]:
    rows = (
        await session.execute(
            select(Report)
            .where(Report.user_id == user.user_id)
            .order_by(Report.generated_time.desc())
            .limit(50)
        )
    ).scalars().all()
    return [
        ReportBrief(
            report_id=r.report_id, title=r.title, status=r.status, generated_time=r.generated_time
        )
        for r in rows
    ]


@router.get("/{report_id}", response_model=ReportOut)
async def get_report(
    report_id: uuid.UUID,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> ReportOut:
    report = (
        await session.execute(
            select(Report)
            .where(Report.report_id == report_id, Report.user_id == user.user_id)
            .options(selectinload(Report.sections), selectinload(Report.sources))
        )
    ).scalar_one_or_none()
    if report is None:
        raise HTTPException(404, "报告不存在")
    return await _build_out(session, report)


@router.delete("/{report_id}")
async def delete_report(
    report_id: uuid.UUID,
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """删除某份报告（章节与溯源关联级联删除，原始数据不受影响）。"""
    report = (
        await session.execute(
            select(Report).where(
                Report.report_id == report_id, Report.user_id == user.user_id
            )
        )
    ).scalar_one_or_none()
    if report is None:
        raise HTTPException(404, "报告不存在")

    # 章节向量没有外键约束，必须显式清掉，否则会留下检索不到出处的孤儿分块
    section_ids = (
        await session.execute(
            select(ReportSection.section_id).where(ReportSection.report_id == report_id)
        )
    ).scalars().all()
    if section_ids:
        await session.execute(
            delete(Embedding).where(
                Embedding.owner_type == "report_section",
                Embedding.owner_id.in_(section_ids),
            )
        )

    await session.delete(report)
    await session.commit()
    return {"ok": True, "deleted": str(report_id)}


@router.post("/{report_id}/translate", response_model=ReportOut)
async def translate_report(
    report_id: uuid.UUID,
    lang: str = "zh",
    user: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> ReportOut:
    """把已有报告就地翻译成指定语言（用于英文模式下查看历史中文报告）。"""
    ok = await ReportService().translate_report(report_id, user.user_id, lang)
    if not ok:
        raise HTTPException(404, "报告不存在")
    return await _load_report_out(session, report_id)


async def _load_report_out(session: AsyncSession, report_id: uuid.UUID) -> ReportOut:
    report = (
        await session.execute(
            select(Report)
            .where(Report.report_id == report_id)
            .options(selectinload(Report.sections), selectinload(Report.sources))
        )
    ).scalar_one()
    return await _build_out(session, report)


async def _build_out(session: AsyncSession, report: Report) -> ReportOut:
    # 溯源：把 report_sources 关联回 raw_data 取标题/URL
    data_ids = [s.data_id for s in report.sources]
    title_map: dict[uuid.UUID, tuple[str, str | None]] = {}
    if data_ids:
        rows = (
            await session.execute(
                select(RawData.data_id, RawData.title, RawData.url).where(
                    RawData.data_id.in_(data_ids)
                )
            )
        ).all()
        title_map = {rid: (title, url) for rid, title, url in rows}

    # 数据新鲜度：最新素材是否早于“当前时间 - 回溯窗口”
    latest_source_time = max(
        (s.data_publish_time for s in report.sources), default=None
    )
    cutoff = datetime.now(timezone.utc) - timedelta(hours=settings.report_lookback_hours)
    data_stale = bool(latest_source_time and latest_source_time < cutoff)

    # 订阅模块的数据新鲜度（用于向用户解释“为什么仍是旧数据”）
    mod_rows = (
        await session.execute(
            select(Module.module_code, func.max(RawData.publish_time))
            .join(UserModulePref, UserModulePref.module_id == Module.module_id)
            .outerjoin(RawData, RawData.module_id == Module.module_id)
            .where(
                UserModulePref.user_id == report.user_id,
                UserModulePref.is_enabled.is_(True),
            )
            .group_by(Module.module_code)
        )
    ).all()
    subscribed = {code for code, _ in mod_rows}
    stale_module_codes = [
        code for code, latest in mod_rows if latest is None or latest < cutoff
    ]

    # 当前有新数据、但用户尚未订阅的模块 → 建议订阅
    all_rows = (
        await session.execute(
            select(Module.module_code, func.max(RawData.publish_time))
            .outerjoin(RawData, RawData.module_id == Module.module_id)
            .group_by(Module.module_code)
        )
    ).all()
    suggest_module_codes = [
        code
        for code, latest in all_rows
        if latest and latest >= cutoff and code not in subscribed
    ]

    return ReportOut(
        report_id=report.report_id,
        title=report.title,
        status=report.status,
        generated_time=report.generated_time,
        data_stale=data_stale,
        latest_source_time=latest_source_time,
        lookback_hours=settings.report_lookback_hours,
        stale_module_codes=stale_module_codes,
        suggest_module_codes=suggest_module_codes,
        sections=[
            SectionOut(section_id=s.section_id, section_type=s.section_type, content=s.content)
            for s in sorted(
                report.sections, key=lambda x: _SECTION_ORDER.get(x.section_type, 9)
            )
        ],
        sources=[
            ReportSourceOut(
                data_id=s.data_id,
                title=title_map.get(s.data_id, (None, None))[0],
                url=title_map.get(s.data_id, (None, None))[1],
                publish_time=s.data_publish_time,
            )
            for s in report.sources
        ],
    )
