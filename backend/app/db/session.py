"""异步数据库引擎与会话工厂。

注意：raw_data / chat_messages 为分区表，插入时若目标月份分区不存在会失败，
      请确保已调用 schema.sql 中的 ensure_monthly_partition() 或由 Airflow 定时预建。
"""
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings

settings = get_settings()

# Serverless / PgBouncer（Neon -pooler）下必须关闭 asyncpg 预编译语句缓存，
# 同时把连接池压小，避免多个函数实例把数据库连接打满。
_connect_args: dict = {}
if "neon.tech" in settings.postgres_dsn or "-pooler" in settings.postgres_dsn:
    _connect_args["statement_cache_size"] = 0

# 连接池：预检 + 限制上限，适配 API 与 Worker 混合负载
engine = create_async_engine(
    settings.postgres_dsn,
    pool_pre_ping=True,
    pool_size=2 if settings.serverless else 10,
    max_overflow=3 if settings.serverless else 20,
    pool_recycle=1800,
    connect_args=_connect_args,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,   # 提交后仍可访问 ORM 属性，避免额外查询
    autoflush=False,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI 依赖注入：每个请求一个会话，异常自动回滚。"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
