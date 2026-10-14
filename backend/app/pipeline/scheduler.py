"""进程内调度器：替代 Airflow 的定时触发（精简栈）。

真实部署时用 Airflow DAG 定时调用 run_source(source_id)；
演示环境下由本调度器按固定间隔遍历启用中的数据源，效果等价、启动零成本。
"""
from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select

from app.core.config import get_settings
from app.core.events import bus
from app.db.models import DataSource
from app.db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)
settings = get_settings()


class PipelineScheduler:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if not settings.scheduler_enabled:
            logger.info("调度器未启用（SCHEDULER_ENABLED=false）")
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._loop())
        logger.info("调度器已启动，间隔 %s 秒", settings.scheduler_interval_seconds)

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        # 延迟导入，避免循环依赖
        from app.services.ingestion_service import IngestionService

        service = IngestionService()
        while not self._stop.is_set():
            try:
                async with AsyncSessionLocal() as session:
                    rows = (
                        await session.execute(
                            select(DataSource.source_id).where(DataSource.status == 1)
                        )
                    ).scalars().all()
                for source_id in rows:
                    await service.run_source(source_id)
            except Exception:
                logger.exception("调度轮次执行失败")
            await bus.publish("system", f"调度器本轮结束，下次将在 {settings.scheduler_interval_seconds}s 后执行")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=settings.scheduler_interval_seconds
                )
            except asyncio.TimeoutError:
                continue


scheduler = PipelineScheduler()
