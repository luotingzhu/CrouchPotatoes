"""数据源管理与抓取触发路由。"""
from __future__ import annotations

import asyncio
import json
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.cache import redis_client
from app.core.events import bus
from app.core.source_names import source_name_en
from app.db.models import DataSource, RawData, User
from app.db.session import get_session
from app.schemas import (
    RawDataOut,
    SourceIn,
    SourceOut,
    TranslateIn,
    TranslateOut,
    TranslatedItem,
    TriggerOut,
)
from app.services.ai_client import chat_complete
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
    await bus.publish(
        "system",
        f"新增数据源：{source.source_name}",
        f"New data source added: {source_name_en(source.source_name)}",
    )
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


_TRANSLATE_CACHE_TTL = 7 * 24 * 3600


@router.post("/raw/translate", response_model=TranslateOut)
async def translate_raw_titles(
    payload: TranslateIn,
    _: User = Depends(get_current_user),
    session: AsyncSession = Depends(get_session),
) -> TranslateOut:
    """把原始数据标题翻译成英文（Redis 缓存 7 天），用于英文界面展示。"""
    ids = payload.data_ids[:30]
    if payload.lang != "en" or not ids:
        return TranslateOut(items=[])

    keys = [f"tr:raw:en:{data_id}" for data_id in ids]
    cached = await redis_client.mget(keys)

    result: dict[str, str] = {}
    missing: list[uuid.UUID] = []
    for data_id, value in zip(ids, cached):
        if value:
            result[str(data_id)] = value
        else:
            missing.append(data_id)

    if missing:
        rows = (
            await session.execute(
                select(RawData.data_id, RawData.title).where(RawData.data_id.in_(missing))
            )
        ).all()
        titles = {str(rid): title for rid, title in rows if title}
        ordered = [data_id for data_id in missing if str(data_id) in titles]

        if ordered:
            system = (
                "你是专业财经新闻翻译。把每条中文新闻标题翻译成简洁、准确的英文，"
                "保留机构名与专有名词。只返回 JSON 字符串数组，顺序与输入一致，不要任何解释。"
            )
            user = json.dumps([titles[str(i)] for i in ordered], ensure_ascii=False)
            try:
                raw = await chat_complete(system, user)
                start, end = raw.find("["), raw.rfind("]")
                parsed = json.loads(raw[start:end + 1]) if start >= 0 and end > start else []
            except Exception:
                parsed = []

            # 只有数量完全对得上才采用，避免标题错位
            if isinstance(parsed, list) and len(parsed) == len(ordered):
                pipe = redis_client.pipeline()
                for data_id, translated in zip(ordered, parsed):
                    if isinstance(translated, str) and translated.strip():
                        text = translated.strip()
                        result[str(data_id)] = text
                        pipe.set(f"tr:raw:en:{data_id}", text, ex=_TRANSLATE_CACHE_TTL)
                await pipe.execute()

    return TranslateOut(
        items=[
            TranslatedItem(data_id=data_id, title=result[str(data_id)])
            for data_id in ids
            if str(data_id) in result
        ]
    )
