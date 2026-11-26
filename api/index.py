"""Vercel Python 入口。

Vercel 的函数目录是 api/，而项目代码在 backend/app，
这里把 backend 加入 sys.path 后直接导出 FastAPI 的 ASGI 应用。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from app.main import app  # noqa: E402,F401  Vercel 通过模块级 `app` 调用 ASGI
