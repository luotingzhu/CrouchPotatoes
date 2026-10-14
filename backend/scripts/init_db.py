r"""初始化数据库：执行 db/schema.sql（建表 + 分区 + 索引 + 字典种子）。

用途：
  * Docker 首次创建数据卷时已自动执行过 schema.sql；
  * 若数据库已存在但表缺失（或重建），可用本脚本手动执行。

运行（在 backend/ 目录下）：
    .\.venv\Scripts\python.exe scripts\init_db.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # 让 app 包可导入

from app.core.config import get_settings  # noqa: E402

SCHEMA_FILE = Path(__file__).resolve().parents[2] / "db" / "schema.sql"


async def main() -> None:
    # asyncpg 需要纯净的 postgresql:// DSN（去掉 SQLAlchemy 的 +asyncpg）
    dsn = get_settings().postgres_dsn.replace("postgresql+asyncpg://", "postgresql://")
    sql = SCHEMA_FILE.read_text(encoding="utf-8")

    print(f"[init_db] 连接 {dsn}")
    conn = await asyncpg.connect(dsn)
    try:
        # asyncpg 无参数 execute 走简单查询协议，可一次执行多语句脚本
        await conn.execute(sql)
        print(f"[init_db] 已执行 {SCHEMA_FILE.name}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
