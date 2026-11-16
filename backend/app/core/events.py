"""进程内事件总线：把管道/服务的关键动作实时推给前端（SSE）。

用途：演示时前端右上角可实时看到「抓取 → 入库 → 向量化 → 报告」的后端同步过程。
实现：每个订阅者一个 asyncio.Queue，发布时广播；队列满则丢弃最旧事件，避免阻塞业务。
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any


class EventBus:
    def __init__(self, max_queue: int = 200) -> None:
        self._subscribers: set[asyncio.Queue] = set()
        self._max_queue = max_queue
        self._history: list[dict[str, Any]] = []   # 保留最近事件，供新订阅者回放

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=self._max_queue)
        # 回放最近历史，让刚打开页面的用户也能看到上下文
        for event in self._history[-20:]:
            q.put_nowait(event)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    def history(self, n: int = 30) -> list[dict[str, Any]]:
        """最近 n 条事件（供状态接口一次性回放）。"""
        return self._history[-n:]

    async def publish(self, stage: str, message: str, message_en: str | None = None, **extra: Any) -> None:
        event = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "stage": stage,          # crawl / ingest / embed / report / chat / system
            "message": message,
            # 英文文案：前端英文界面优先展示；未提供时回退中文
            "message_en": message_en or message,
            **extra,
        }
        self._history.append(event)
        if len(self._history) > 300:
            self._history = self._history[-300:]
        for q in list(self._subscribers):
            if q.full():                       # 丢最旧，保证不阻塞
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            q.put_nowait(event)


bus = EventBus()
