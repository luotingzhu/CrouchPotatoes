"""异步数据库引擎与会话工厂。

注意：raw_data / chat_messages 为分区表，插入时若目标月份分区不存在会失败，
      请确保已调用 schema.sql 中的 ensure_monthly_partition() 或由 Airflow 定时预建。
"""
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings

settings = get_settings()

# 连接池：预检 + 限制上限，适配 API 与 Worker 混合负载
engine = create_async_engine(
    settings.postgres_dsn,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800,
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
