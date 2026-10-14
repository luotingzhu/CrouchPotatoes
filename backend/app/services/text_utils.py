"""共享文本工具：切块 + 指纹。被数据入库与报告向量化共用。"""
import hashlib
from typing import Iterable


def sha256_hash(text: str) -> str:
    """生成内容指纹，用于原始数据去重。"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def chunk_text(text: str, size: int = 500, overlap: int = 80) -> list[str]:
    """按字符长度切块（中文友好），相邻块保留 overlap 重叠，避免语义被截断。

    生产环境可替换为按标点/句子的递归切分，或直接使用 token 级切分。
    """
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= size:
        return [text]

    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        chunks.append(text[start:end])
        if end == len(text):
            break
        start = end - overlap  # 回退 overlap 个字符形成重叠窗口
    return chunks


def join_blocks(blocks: Iterable[str]) -> str:
    """拼接文本块，过滤空值。"""
    return "\n".join(b for b in blocks if b)
