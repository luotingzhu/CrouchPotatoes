r"""种子数据：演示用数据源 + 演示账号。

运行（在 backend/ 目录下）：
    .\.venv\Scripts\python.exe scripts\seed_demo.py

说明：
  * 演示数据源指向本服务的本地模拟资讯接口（真实走 HTTP 抓取，内容可控）；
  * 需先启动后端服务（uvicorn 监听 8000），抓取时才能取到数据。
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # 让 app 包可导入

from app.core.security import hash_password  # noqa: E402
from app.db.models import DataSource, Module, Role, User, UserModulePref, UserRole  # noqa: E402
from app.db.session import AsyncSessionLocal  # noqa: E402

BASE = "http://127.0.0.1:8000"

DEMO_SOURCES = [
    ("示例资讯源 · 新闻", f"{BASE}/api/demo/feed/news", "api"),
    ("示例资讯源 · 政治", f"{BASE}/api/demo/feed/politics", "api"),
    ("示例资讯源 · 商业统计", f"{BASE}/api/demo/feed/business", "api"),
    ("示例资讯源 · 金融", f"{BASE}/api/demo/feed/finance", "api"),
]

DEMO_USER = {
    "username": "demo",
    "email": "demo@domel.local",
    "password": "demo1234",
    "roles": ["Manager"],
    "modules": ["news", "politics", "business", "finance"],
}


async def main() -> None:
    async with AsyncSessionLocal() as session:
        # 1) 数据源（按 URL 去重）
        for name, url, stype in DEMO_SOURCES:
            exists = (
                await session.execute(select(DataSource).where(DataSource.url == url))
            ).scalar_one_or_none()
            if exists is None:
                session.add(DataSource(source_name=name, url=url, source_type=stype, status=1))
                print(f"[seed] 新增数据源：{name}")

        # 2) 演示用户
        user = (
            await session.execute(select(User).where(User.username == DEMO_USER["username"]))
        ).scalar_one_or_none()
        if user is None:
            user = User(
                username=DEMO_USER["username"],
                email=DEMO_USER["email"],
                password_hash=hash_password(DEMO_USER["password"]),
            )
            session.add(user)
            await session.flush()

            role_rows = (await session.execute(select(Role.role_id, Role.role_name))).all()
            rmap = {name: rid for rid, name in role_rows}
            for rn in DEMO_USER["roles"]:
                if rn in rmap:
                    session.add(UserRole(user_id=user.user_id, role_id=rmap[rn]))

            mod_rows = (await session.execute(select(Module.module_id, Module.module_code))).all()
            mmap = {code: mid for mid, code in mod_rows}
            for mc in DEMO_USER["modules"]:
                if mc in mmap:
                    session.add(UserModulePref(user_id=user.user_id, module_id=mmap[mc]))

            print(f"[seed] 新增演示用户：{DEMO_USER['username']} / {DEMO_USER['password']}")

        await session.commit()
    print("[seed] 完成")


if __name__ == "__main__":
    asyncio.run(main())
