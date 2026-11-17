"""数据源管理与抓取触发路由。"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.cache import redis_client
from app.core.config import get_settings
from app.core.events import bus
from app.db.models import DataSource, Embedding, Module, RawData, Report, User
from app.db.session import get_session
from app.schemas import PurgeIn, PurgeOut, RawDataOut, SourceIn, SourceOut, TriggerOut
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


# 示例演示源已停用：不再在抓取时自动补齐，避免清空后又冒出来。


@router.post("/trigger-all", response_model=list[TriggerOut])
async def trigger_all(
    request: Request,
    _: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> list[TriggerOut]:
    """一键抓取所有启用的数据源。"""
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


@router.get("/stats")
async def ingest_stats(
    _: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> dict:
    """入库概览：总量、时间范围、按模块与按来源的分布。"""
    total, first_at, last_at = (
        await session.execute(
            select(func.count(), func.min(RawData.publish_time), func.max(RawData.publish_time))
        )
    ).one()

    by_module = (
        await session.execute(
            select(Module.module_code, Module.module_name, func.count())
            .join(RawData, RawData.module_id == Module.module_id)
            .group_by(Module.module_code, Module.module_name)
            .order_by(func.count().desc())
        )
    ).all()

    by_source = (
        await session.execute(
            select(
                DataSource.source_name,
                DataSource.source_type,
                func.count(),
                func.max(RawData.publish_time),
            )
            .join(RawData, RawData.source_id == DataSource.source_id)
            .group_by(DataSource.source_name, DataSource.source_type)
            .order_by(func.count().desc())
        )
    ).all()

    vectors = (
        await session.execute(
            select(func.count()).select_from(Embedding)
            .where(Embedding.owner_type == "raw_data")
        )
    ).scalar_one()

    return {
        "total": total or 0,
        "sources": len(by_source),
        "vectors": vectors or 0,
        "first_at": first_at.isoformat() if first_at else None,
        "latest_at": last_at.isoformat() if last_at else None,
        "by_module": [
            {"code": c, "name": n, "rows": cnt} for c, n, cnt in by_module
        ],
        "by_source": [
            {"name": name, "type": stype, "rows": cnt,
             "latest": latest.isoformat() if latest else None}
            for name, stype, cnt, latest in by_source
        ],
    }


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


@router.post("/purge", response_model=PurgeOut)
async def purge_crawled_data(
    payload: PurgeIn,
    _: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> PurgeOut:
    """清空抓取数据，便于重新抓取与重新生成报告。

    删除：原始数据、对应的向量分块、所有报告（含章节与溯源关联）。
    保留：数据源配置、账号、对话历史。
    需要管理员密码，避免误触或被其他账号清库。
    """
    if payload.password != get_settings().admin_password:
        raise HTTPException(403, "管理员密码不正确")

    raw_count = (await session.execute(
        select(func.count()).select_from(RawData))).scalar_one()
    report_count = (await session.execute(
        select(func.count()).select_from(Report))).scalar_one()
    vector_count = (await session.execute(
        select(func.count()).select_from(Embedding)
        .where(Embedding.owner_type.in_(("raw_data", "report_section"))))).scalar_one()

    # 报告删掉时会级联清掉 report_sections / report_sources
    await session.execute(delete(Report))
    await session.execute(
        delete(Embedding).where(Embedding.owner_type.in_(("raw_data", "report_section")))
    )
    await session.execute(delete(RawData))
    await session.commit()

    # 问答缓存基于旧数据，一并失效
    async for key in redis_client.scan_iter("chat:a:*"):
        await redis_client.delete(key)

    await bus.publish("system", f"已清空抓取数据：{raw_count} 条原始数据、{report_count} 份报告")

    return PurgeOut(raw_rows=raw_count, reports=report_count, vectors=vector_count)
