"""全局配置：数据库 / Redis / Kafka / 向量库 / 模型 连接参数。

统一走环境变量（12-Factor），便于在 Airflow、Kafka Worker、API 服务间复用。
"""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ---------- PostgreSQL（主库：业务 + JSONB + 全文检索 + pgvector） ----------
    postgres_dsn: str = "postgresql+asyncpg://domel:domel@localhost:5432/domel"

    # ---------- Redis（Session + Chat 短期上下文 + 查询缓存） ----------
    redis_url: str = "redis://localhost:6379/0"
    redis_chat_context_ttl: int = 3600      # 会话短期上下文存活时间（秒）
    redis_qa_cache_ttl: int = 1800          # 问答结果缓存时间（秒）

    # ---------- 消息中间件（精简栈默认内存队列；有 Kafka 时改为 kafka） ----------
    broker_backend: str = "memory"          # memory | kafka
    kafka_bootstrap: str = "localhost:9092"
    kafka_topic_raw: str = "domel.raw_data"
    kafka_topic_embed: str = "domel.embed"
    kafka_group_ingest: str = "domel-ingest"

    # ---------- 进程内调度（替代 Airflow 的定时触发） ----------
    scheduler_enabled: bool = False
    scheduler_interval_seconds: int = 300

    # ---------- 向量数据库（三选一） ----------
    vector_backend: str = "pgvector"        # pgvector | qdrant | milvus
    qdrant_url: str = "http://localhost:6333"
    milvus_uri: str = "http://localhost:19530"
    vector_collection: str = "domel_chunks"

    # ---------- 对话模型（OpenAI 兼容协议） ----------
    openai_api_key: str = ""                # 留空则回退读取系统环境变量 OPENAI_API_KEY
    openai_base_url: str = ""               # 留空用官方；兼容 DeepSeek/通义/Moonshot/自建网关
    llm_model: str = "gpt-4o-mini"

    # ---------- 向量模型（可与对话使用不同服务商；此处留空则复用上面的 key/base_url） ----------
    embedding_api_key: str = ""
    embedding_base_url: str = ""
    embedding_model: str = "text-embedding-3-small"
    embedding_dim: int = 1536               # 必须与 pgvector vector(N) 一致

    # ---------- 管理员模式（用于解锁“后端同步状态”模块） ----------
    admin_password: str = "Ch11seburger"
    admin_session_ttl: int = 8 * 3600       # 解锁有效期（秒）

    # ---------- 报告生成窗口 ----------
    report_lookback_hours: int = 24         # 每次生成报告回溯的原始数据时间窗


@lru_cache
def get_settings() -> Settings:
    """进程内单例，避免重复解析环境变量。"""
    return Settings()
