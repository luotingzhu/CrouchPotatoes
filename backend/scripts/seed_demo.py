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

# 真实财经资讯站点（爬虫）。
# 这些站点均为 JS 渲染或结构特殊，已在 app/services/ingestion_service.py 的 _SITE_PROFILES
# 中按域名配置了列表选择器；抓取时走「列表页 → 文章页正文」。
REAL_SOURCES = [
    ("财经资讯 · 东方财富", "https://finance.eastmoney.com/a/ccjdd.html", "crawler"),
    ("财经资讯 · 新浪财经", "https://finance.sina.com.cn/", "crawler"),
    ("财经资讯 · 第一财经", "https://www.yicai.com/news/", "crawler"),
    ("财经资讯 · 凤凰财经", "https://finance.ifeng.com/", "crawler"),
    # 国际 / 全球市场来源
    ("国际财经 · CNBC", "https://www.cnbc.com/world/?region=world", "crawler"),
    ("国际财经 · 智通财经", "https://www.zhitongcaijing.com/", "crawler"),
    ("国际财经 · 格隆汇", "https://www.gelonghui.com/", "crawler"),
    ("国际财经 · 东方财富国际", "https://finance.eastmoney.com/a/cgjjj.html", "crawler"),
]

ALL_SOURCES = DEMO_SOURCES + REAL_SOURCES

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
        for name, url, stype in ALL_SOURCES:
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
