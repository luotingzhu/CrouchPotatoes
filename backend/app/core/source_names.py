"""数据源名称的英文显示名。

数据源名称存在数据库里（中文），日志与英文界面需要英文名时统一走这里；
未收录的名称原样返回，避免影响用户自建的源。
"""
from __future__ import annotations

_SOURCE_NAME_EN: dict[str, str] = {
    "财经资讯 · 东方财富": "Finance News · East Money",
    "财经资讯 · 新浪财经": "Finance News · Sina Finance",
    "财经资讯 · 第一财经": "Finance News · Yicai",
    "财经资讯 · 凤凰财经": "Finance News · Phoenix Finance",
    "政策时政 · 新华网": "Policy & Politics · Xinhua",
    "商业统计 · 国家统计局": "Business Statistics · NBS",
    "国际财经 · CNBC": "Global Markets · CNBC",
    "国际财经 · 智通财经": "Global Markets · Zhitong Finance",
    "国际财经 · 格隆汇": "Global Markets · Gelonghui",
    "国际财经 · 东方财富国际": "Global Markets · East Money Global",
}


def source_name_en(name: str) -> str:
    return _SOURCE_NAME_EN.get(name, name)
