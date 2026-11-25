"""AI 客户端封装：Embedding 与 Chat Completion（两套独立配置）。

为什么拆成两套：
  不少国内服务商（如 DeepSeek）只提供对话接口、没有 embeddings 接口，
  因此对话与向量可以分别指向不同服务商：各自有独立的 key / base_url / model。

注意：客户端**懒加载**，未配置 key 时导入不会崩溃，只在真正调用时报明确错误。
"""
from functools import lru_cache
import re

from openai import AsyncOpenAI

from app.core.config import get_settings

settings = get_settings()

# 前端按纯文本渲染回答，模型自带 Markdown 时星号会直接露出来，这里统一清洗
_MD_BOLD = re.compile(r"\*\*(.+?)\*\*", re.S)
_MD_UNDER = re.compile(r"(?<![\w])__(.+?)__(?![\w])", re.S)
_MD_ITALIC = re.compile(r"(?<![\w*])\*(?!\s)([^*\n]{1,80}?)(?<!\s)\*(?![\w*])")
_MD_HEADING = re.compile(r"(?m)^[ \t]{0,3}#{1,6}[ \t]*")
_MD_CODE = re.compile(r"`([^`\n]+)`")


def plain_text(text: str) -> str:
    """去掉 Markdown 强调/标题标记，保留正文与 [n] 引用编号。"""
    text = _MD_BOLD.sub(r"\1", text)
    text = _MD_UNDER.sub(r"\1", text)
    text = _MD_ITALIC.sub(r"\1", text)
    text = _MD_HEADING.sub("", text)
    text = _MD_CODE.sub(r"\1", text)
    return text


@lru_cache
def _chat_client() -> AsyncOpenAI:
    if not settings.openai_api_key:
        raise RuntimeError(
            "未配置对话模型：请在 backend/.env 设置 OPENAI_API_KEY（配合 OPENAI_BASE_URL 使用兼容网关）"
        )
    return AsyncOpenAI(
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url or None,
    )


@lru_cache
def _embed_client() -> AsyncOpenAI:
    # 未单独配置向量服务商时，复用对话服务的 key/base_url
    key = settings.embedding_api_key or settings.openai_api_key
    base = settings.embedding_base_url or settings.openai_base_url
    if not key:
        raise RuntimeError(
            "未配置向量模型：请在 backend/.env 设置 EMBEDDING_API_KEY（或 OPENAI_API_KEY）"
        )
    return AsyncOpenAI(api_key=key, base_url=base or None)


async def embed_texts(texts: list[str], model: str | None = None) -> list[list[float]]:
    """批量文本向量化，返回与输入等长的向量列表。"""
    if not texts:
        return []
    resp = await _embed_client().embeddings.create(
        model=model or settings.embedding_model,
        input=texts,
    )
    # 按 index 排序，保证与输入顺序一致
    return [item.embedding for item in sorted(resp.data, key=lambda d: d.index)]


async def chat_complete(system: str, user: str, model: str | None = None) -> str:
    """单轮补全；报告合成场景可改用 stream / JSON mode。"""
    resp = await _chat_client().chat.completions.create(
        model=model or settings.llm_model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
    )
    return plain_text(resp.choices[0].message.content or "")
