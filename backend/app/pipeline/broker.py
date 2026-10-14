"""消息中间件抽象层。

精简栈（课堂演示）默认使用 MemoryBroker：进程内 asyncio 队列，零外部依赖、稳定；
保留 KafkaBroker 以对接真实 Kafka（架构不变，仅切换配置 BROKER_BACKEND=kafka）。

业务代码只依赖 Broker 抽象，因此「抓取端投递 / 消费端入库」的解耦逻辑完全一致。
"""
from __future__ import annotations

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class Broker(ABC):
    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def publish(self, topic: str, payload: dict[str, Any]) -> None: ...

    @abstractmethod
    def subscribe(self, topic: str) -> AsyncIterator[dict[str, Any]]: ...


class MemoryBroker(Broker):
    """进程内广播队列：单进程演示用。"""

    def __init__(self) -> None:
        self._queues: dict[str, list[asyncio.Queue]] = {}

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        self._queues.clear()

    async def publish(self, topic: str, payload: dict[str, Any]) -> None:
        for q in self._queues.get(topic, []):
            await q.put(payload)

    async def subscribe(self, topic: str) -> AsyncIterator[dict[str, Any]]:
        q: asyncio.Queue = asyncio.Queue()
        self._queues.setdefault(topic, []).append(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._queues.get(topic, []).remove(q)


class KafkaBroker(Broker):
    """真实 Kafka 实现（可选）。切换 BROKER_BACKEND=kafka 生效。"""

    def __init__(self) -> None:
        self._producer = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap,
            acks="all",
            enable_idempotence=True,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

    async def publish(self, topic: str, payload: dict[str, Any]) -> None:
        await self._producer.send_and_wait(
            topic, json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        )

    async def subscribe(self, topic: str) -> AsyncIterator[dict[str, Any]]:
        from aiokafka import AIOKafkaConsumer

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id=settings.kafka_group_ingest,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        try:
            async for msg in consumer:
                yield json.loads(msg.value.decode("utf-8"))
        finally:
            await consumer.stop()


_broker: Broker | None = None


def get_broker() -> Broker:
    """Broker 单例（按配置选择实现）。"""
    global _broker
    if _broker is None:
        if settings.broker_backend == "kafka":
            _broker = KafkaBroker()
        else:
            _broker = MemoryBroker()
    return _broker
