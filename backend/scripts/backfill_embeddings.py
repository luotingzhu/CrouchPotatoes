r"""为历史数据补向量（raw_data / report_section）。

用途：更换 embedding 模型或新增数据源后，把那些尚未向量化的历史记录补齐，
      使其能被 RAG 检索到。脚本幂等：已有向量的记录会被跳过。

运行（在 backend/ 目录下）：
    .\.venv\Scripts\python.exe scripts\backfill_embeddings.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # 让 app 包可导入

from app.core.config import get_settings  # noqa: E402
from app.db.models import Embedding, RawData, ReportSection  # noqa: E402
from app.db.session import AsyncSessionLocal  # noqa: E402
from app.services.ai_client import embed_texts  # noqa: E402
from app.services.text_utils import chunk_text  # noqa: E402

settings = get_settings()


async def backfill_raw_data(session, done: set[tuple[str, object]]) -> int:
    rows = (await session.execute(select(RawData))).scalars().all()
    added = 0
    for row in rows:
        if ("raw_data", row.data_id) in done:
            continue
        chunks = chunk_text(f"{row.title}\n{row.content or ''}")
        if not chunks:
            continue
        vectors = await embed_texts(chunks)
        session.add_all(
            Embedding(
                owner_type="raw_data",
                owner_id=row.data_id,
                chunk_index=i,
                chunk_text=text,
                embedding=vec,
                model_name=settings.embedding_model,
            )
            for i, (text, vec) in enumerate(zip(chunks, vectors))
        )
        added += len(chunks)
        print(f"  raw_data  {row.title[:36]} → {len(chunks)} 块")
    return added


async def backfill_report_sections(session, done: set[tuple[str, object]]) -> int:
    rows = (await session.execute(select(ReportSection))).scalars().all()
    added = 0
    for row in rows:
        if ("report_section", row.section_id) in done:
            continue
        chunks = chunk_text(row.content)
        if not chunks:
            continue
        vectors = await embed_texts(chunks)
        session.add_all(
            Embedding(
                owner_type="report_section",
                owner_id=row.section_id,
                chunk_index=i,
                chunk_text=text,
                embedding=vec,
                model_name=settings.embedding_model,
            )
            for i, (text, vec) in enumerate(zip(chunks, vectors))
        )
        added += len(chunks)
        print(f"  report_section  {row.section_type} → {len(chunks)} 块")
    return added


async def main() -> None:
    print(f"[backfill] 向量模型 = {settings.embedding_model} (dim={settings.embedding_dim})")
    async with AsyncSessionLocal() as session:
        # 已向量化的 (owner_type, owner_id) 集合，用于跳过
        done = {
            (t, i) for t, i in (await session.execute(select(Embedding.owner_type, Embedding.owner_id))).all()
        }
        print(f"[backfill] 已有向量记录 {len(done)} 条")

        n1 = await backfill_raw_data(session, done)
        n2 = await backfill_report_sections(session, done)
        await session.commit()
        print(f"[backfill] 新增向量分块：raw_data {n1}，report_section {n2}")


if __name__ == "__main__":
    asyncio.run(main())
