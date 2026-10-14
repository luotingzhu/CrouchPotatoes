"""数据源管理与抓取触发路由。"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.events import bus
from app.db.models import DataSource, RawData, User
from app.db.session import get_session
from app.schemas import RawDataOut, SourceIn, SourceOut, TriggerOut
from app.services.ingestion_service import IngestionService

router = APIRouter(prefix="/api/sources", tags=["sources"])


@router.get("", response_model=list[SourceOut])
async def list_sources(
    _: User = Depends(get_current_user), session: AsyncSession = Depends(get_session)
) -> list[SourceOut]:
    rows = (
        await session.execute(select(DataSource).order_by(DataSource.source_id))
    ).scalars().all()
    return [
        SourceOut(
            source_id=s.source_id,
            source_name=s.source_name,
            url=s.url,
            source_type=s.source_type,
            status=s.status,
        )
        for s in rows
    ]


@router.post("", response_model=SourceOut)
async def create_source(
    payload: SourceIn,
    _: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> SourceOut:
    source = DataSource(
        source_name=payload.source_name,
        url=payload.url,
        source_type=payload.source_type,
        status=1,
    )
    session.add(source)
    await session.commit()
    await bus.publish("system", f"新增数据源：{source.source_name}")
    return SourceOut(
        source_id=source.source_id,
        source_name=source.source_name,
        url=source.url,
        source_type=source.source_type,
        status=source.status,
    )


@router.post("/{source_id}/trigger", response_model=TriggerOut)
async def trigger_source(
    source_id: int,
    _: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> TriggerOut:
    """手动触发一次抓取（等价于 Airflow 手动跑一个 DAG）。"""
    exists = await session.get(DataSource, source_id)
    if exists is None:
        raise HTTPException(404, "数据源不存在")

    service = IngestionService()
    # 抓取投递到 Broker 后立即返回；入库由常驻消费者异步完成，前端可通过事件流观察
    task = asyncio.create_task(service.run_source(source_id))
    sent = await task
    return TriggerOut(source_id=source_id, sent=sent)


@router.post("/trigger-all", response_model=list[TriggerOut])
async def trigger_all(
    _: User = Depends(get_current_user), session: AsyncSession = Depends(get_session)
) -> list[TriggerOut]:
    """一键触发所有启用中的数据源（演示用）。"""
    ids = (
        await session.execute(
            select(DataSource.source_id).where(DataSource.status == 1)
        )
    ).scalars().all()

    service = IngestionService()
    results: list[TriggerOut] = []
    for sid in ids:
        sent = await service.run_source(sid)
        results.append(TriggerOut(source_id=sid, sent=sent))
    return results


@router.get("/raw", response_model=list[RawDataOut])
async def recent_raw_data(
    limit: int = 30,
    _: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[RawDataOut]:
    """最近入库的原始数据（观察入库结果）。"""
    rows = (
        await session.execute(
            select(RawData).order_by(RawData.publish_time.desc()).limit(min(limit, 100))
        )
    ).scalars().all()
    return [
        RawDataOut(
            data_id=r.data_id,
            title=r.title,
            module_id=r.module_id,
            publish_time=r.publish_time,
            url=r.url,
        )
        for r in rows
    ]
