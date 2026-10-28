r"""FastAPI 应用入口：路由装配 + 生命周期（启动消费者/调度器）。

本地启动：
    cd backend
    .\.venv\Scripts\python.exe -m uvicorn app.main:app --reload --port 8000
然后浏览器打开 http://127.0.0.1:8000/
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api import auth, chat, demo, prefs, reports, sources, system
from app.core.config import get_settings
from app.core.events import bus
from app.db.session import engine
from app.pipeline.broker import get_broker
from app.pipeline.scheduler import scheduler
from app.services.ingestion_service import IngestionService

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

settings = get_settings()
STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """启动：拉起 Broker、常驻入库消费者、进程内调度器。"""
    broker = get_broker()
    await broker.start()

    consumer_task = asyncio.create_task(IngestionService().consume_loop())
    await scheduler.start()
    await bus.publish("system", "后端服务已启动，消费者与调度器就绪")

    try:
        yield
    finally:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        await scheduler.stop()
        await broker.stop()
        await engine.dispose()


app = FastAPI(title="Domel 电机市场情报 AI 助手", version="1.0.0", lifespan=lifespan)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """把内部异常转成可读信息返回前端，避免只看到「请求失败 500」。

    上游 AI 服务错误（鉴权失败、模型不存在、网络不通）归类为 502，
    便于前端区分「服务端 bug」与「外部依赖不可用」。
    """
    logger.exception("未处理异常 %s %s", request.method, request.url.path)

    status_code = 500
    try:
        import openai

        if isinstance(exc, openai.OpenAIError):
            status_code = 502
    except Exception:  # noqa: BLE001
        pass

    return JSONResponse(status_code=status_code, content={"detail": f"{type(exc).__name__}: {exc}"})

# ---------- 路由 ----------
app.include_router(auth.router)
app.include_router(prefs.router)
app.include_router(sources.router)
app.include_router(reports.router)
app.include_router(chat.router)
app.include_router(system.router)
app.include_router(demo.router)


@app.get("/api/health")
async def health() -> dict:
    """公开健康检查（供前端顶栏展示运行环境，不含敏感数据）。"""
    return {
        "ok": True,
        "vector_backend": settings.vector_backend,
        "broker_backend": settings.broker_backend,
    }


# ---------- 前端静态页（单页 HTML，原生 JS） ----------
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")
