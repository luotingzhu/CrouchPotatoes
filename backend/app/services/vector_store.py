"""向量存储抽象层：pgvector（库内）/ Qdrant / Milvus 三选一，业务层无感知。

选择依据：
  * 数据量小、要求与业务强一致（事务内可回滚）→ pgvector；
  * 亿级向量、需专用 ANN 集群 → Milvus / Qdrant。
"""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models import Embedding

settings = get_settings()


@dataclass
class VectorItem:
    """待写入的向量条目。"""
    owner_type: str
    owner_id: uuid.UUID
    chunk_index: int
    chunk_text: str
    embedding: list[float]


@dataclass
class VectorHit:
    """检索命中结果。"""
    owner_type: str
    owner_id: str
    chunk_text: str
    score: float                       # 余弦相似度，越大越相关
    payload: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# 抽象接口
# ---------------------------------------------------------------------------
class BaseVectorStore(ABC):
    @abstractmethod
    async def upsert(self, items: list[VectorItem]) -> None:
        """批量写入/更新向量。"""

    @abstractmethod
    async def search(
        self,
        embedding: list[float],
        top_k: int = 5,
        owner_types: Optional[list[str]] = None,
    ) -> list[VectorHit]:
        """按向量相似度检索 Top-K。"""


# ---------------------------------------------------------------------------
# 实现一：pgvector（直接复用业务库，事务内一致）
# ---------------------------------------------------------------------------
class PgVectorStore(BaseVectorStore):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(self, items: list[VectorItem]) -> None:
        self.session.add_all(
            Embedding(
                owner_type=it.owner_type,
                owner_id=it.owner_id,
                chunk_index=it.chunk_index,
                chunk_text=it.chunk_text,
                embedding=it.embedding,
                model_name=settings.embedding_model,
            )
            for it in items
        )
        await self.session.flush()   # 由外层事务统一提交

    async def search(
        self,
        embedding: list[float],
        top_k: int = 5,
        owner_types: Optional[list[str]] = None,
    ) -> list[VectorHit]:
        # 余弦距离 <=> 1 - 相似度；使用 ix_embeddings_hnsw 索引
        distance = Embedding.embedding.cosine_distance(embedding)
        stmt = (
            select(Embedding, (1 - distance).label("score"))
            .order_by(distance)
            .limit(top_k)
        )
        if owner_types:
            stmt = stmt.where(Embedding.owner_type.in_(owner_types))

        rows = (await self.session.execute(stmt)).all()
        return [
            VectorHit(
                owner_type=emb.owner_type,
                owner_id=str(emb.owner_id),
                chunk_text=emb.chunk_text,
                score=float(score),
            )
            for emb, score in rows
        ]


# ---------------------------------------------------------------------------
# 实现二：Qdrant（外置专用向量库，示意实现）
# ---------------------------------------------------------------------------
class QdrantVectorStore(BaseVectorStore):
    def __init__(self) -> None:
        from qdrant_client import AsyncQdrantClient  # 延迟导入，按需启用

        self.client = AsyncQdrantClient(url=settings.qdrant_url)
        self.collection = settings.vector_collection

    async def upsert(self, items: list[VectorItem]) -> None:
        from qdrant_client.models import PointStruct

        points = [
            PointStruct(
                id=str(uuid.uuid4()),
                vector=it.embedding,
                payload={
                    "owner_type": it.owner_type,
                    "owner_id": str(it.owner_id),
                    "chunk_index": it.chunk_index,
                    "chunk_text": it.chunk_text,
                },
            )
            for it in items
        ]
        await self.client.upsert(collection_name=self.collection, points=points)

    async def search(
        self,
        embedding: list[float],
        top_k: int = 5,
        owner_types: Optional[list[str]] = None,
    ) -> list[VectorHit]:
        from qdrant_client.models import Filter, FieldCondition, MatchAny

        query_filter = None
        if owner_types:
            query_filter = Filter(
                must=[FieldCondition(key="owner_type", match=MatchAny(any=owner_types))]
            )
        hits = await self.client.search(
            collection_name=self.collection,
            query_vector=embedding,
            limit=top_k,
            query_filter=query_filter,
        )
        return [
            VectorHit(
                owner_type=h.payload["owner_type"],
                owner_id=h.payload["owner_id"],
                chunk_text=h.payload["chunk_text"],
                score=float(h.score),
                payload=h.payload,
            )
            for h in hits
        ]


# ---------------------------------------------------------------------------
# 工厂
# ---------------------------------------------------------------------------
def get_vector_store(session: Optional[AsyncSession] = None) -> BaseVectorStore:
    """按配置返回向量库实现。pgvector 复用业务会话，保证一致性。"""
    if settings.vector_backend == "pgvector":
        if session is None:
            raise ValueError("pgvector 后端需要传入 AsyncSession")
        return PgVectorStore(session)
    if settings.vector_backend == "qdrant":
        return QdrantVectorStore()
    raise ValueError(f"未支持的向量后端: {settings.vector_backend}")
