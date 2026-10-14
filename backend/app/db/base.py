"""SQLAlchemy 声明式基类。

统一约束命名规则，保证 Alembic 迁移脚本稳定、可预测。
"""
from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

# 约定式命名：{类型}_{表}_{列}，避免自动生成随机名导致迁移漂移
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
