"""AI 报告路由：生成、列表、详情（含溯源）。"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.models import RawData, Report, User
from app.db.session import get_session
from app.schemas import ReportBrief, ReportOut, ReportSourceOut, SectionOut
from app.services.report_service import ReportService

router = APIRouter(prefix="/api/reports", tags=["reports"])

# 报告段落的展示顺序（摘要 → 经济 → 政策），避免按字母序排导致阅读顺序错乱
_SECTION_ORDER = {"summary": 0, "economic": 1, "policy": 2}


@router.post("/generate", response_model=ReportOut)
async def generate_report(
    user: User = Depends(get_current_user), session: AsyncSession = Depends(get_session)
) -> ReportOut:
    """按当前用户的角色 + 订阅模块生成一份定制报告（真实调用 LLM + Embedding）。"""
    report_id = await ReportService().generate_for_user(user.user_id)
    if report_id is None:
        raise HTTPException(400, "暂无可用于生成报告的数据：请先配置模块并抓取数据")
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

    return ReportOut(
        report_id=report.report_id,
        title=report.title,
        status=report.status,
        generated_time=report.generated_time,
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
