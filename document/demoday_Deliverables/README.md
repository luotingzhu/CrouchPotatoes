# FinBrief · Motor Market Intelligence AI Assistant

A market-intelligence assistant for B2B electric-motor companies: multi-source ingestion → partitioned storage → vectorisation → role-aware, traceable reports → retrieval-augmented Q&A over the knowledge base.

> Originally named Domel; renamed FinBrief in November 2026.

[English](#english) · [中文](#中文)

---

## English

### Features

- **Multi-source ingestion** — per-domain parsers for real sources (Sina Finance, Yicai, Gelonghui, CNBC, Xinhua, National Bureau of Statistics) plus JSON API sources
- **Storage** — `raw_data` partitioned by month, de-duplicated on `(source_id, content_hash)`, embedded only after commit
- **AI reports** — tailored to role (Manager / Sales / Tech) and subscribed modules, with a table of contents and chapters; per-module quotas stop high-volume news feeds from crowding out low-frequency statistics; every claim traces back to source articles
- **Knowledge-base chat** — pgvector retrieval with Redis cache-aside and short-term context; answers come with structured citations (title, original URL, matched snippet)
- **Bilingual UI** — CN/EN across the app, including database-sourced names, report metadata and log events
- **Admin mode** — password-unlocked "Backend Sync" panel with event stream, table counts, partitions and vector counts
- **Live pipeline view** — SSE stream of crawl, ingest, embed, report and chat events

### Stack

| Layer | Choice |
|---|---|
| Backend | Python 3.12 · FastAPI · SQLAlchemy 2.0 async (asyncpg) · Pydantic v2 |
| Database | PostgreSQL 16 + pgvector (monthly partitions, GIN full-text/JSONB, HNSW) |
| Cache | Redis 7 (session tokens, answer cache, chat context, admin flag, translation cache) |
| Pipeline | In-process broker (Kafka-ready) and scheduler (replaces Airflow) |
| Models | Chat: DeepSeek `deepseek-chat` (OpenAI-compatible) · Embeddings: `BAAI/bge-m3` (1024-d) |
| Frontend | Single-page HTML + vanilla JS, no build step |

### Quick start (Windows)

Requirements: Windows 10/11, WSL2, Docker (containers run inside WSL), Python 3.12.

```powershell
copy backend\.env.example backend\.env     # fill in OPENAI_API_KEY and EMBEDDING_API_KEY
start.bat                                   # containers → health checks → backend → URLs
```

- App: <http://127.0.0.1:8000/>
- Landing page: <http://127.0.0.1:8000/welcome>
- Demo account: `demo / demo1234` (run the seed script once)

Manual start:

```powershell
wsl -e bash -lc "cd /mnt/t/.../CrouchPotatoes && docker compose up -d"
cd backend
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Optional seeding:

```powershell
.\.venv\Scripts\python.exe scripts\seed_demo.py
.\.venv\Scripts\python.exe scripts\backfill_embeddings.py
```

> An idle WSL VM shuts down and takes the containers with it. `start.ps1` keeps WSL alive and checks both PostgreSQL and Redis health.

### Configuration (`backend/.env`)

| Variable | Purpose |
|---|---|
| `POSTGRES_DSN` | `postgresql+asyncpg://domel:domel@localhost:5432/domel` |
| `REDIS_URL` | `redis://localhost:6379/0` |
| `OPENAI_API_KEY` / `OPENAI_BASE_URL` / `LLM_MODEL` | Chat model (DeepSeek by default) |
| `EMBEDDING_*` | Embedding provider; `EMBEDDING_DIM` must match `vector(N)` in the DDL |
| `BROKER_BACKEND` | `memory` (default) or `kafka` |
| `SCHEDULER_ENABLED` / `SCHEDULER_INTERVAL_SECONDS` | In-process scheduled crawling |
| `REPORT_LOOKBACK_HOURS` | Report data window |
| `VECTOR_BACKEND` | `pgvector` / `qdrant` / `milvus` |
| `ADMIN_PASSWORD` | Admin-mode password |

### Layout

```
backend/app/api/        # auth, sources, reports, chat, system, demo
backend/app/core/       # settings, event bus, cache, security, source-name mapping
backend/app/db/         # ORM models and session
backend/app/pipeline/   # broker and scheduler
backend/app/services/   # ingestion, reports, RAG, AI client, vector store
backend/app/static/     # SPA, landing page, logo
backend/scripts/        # init, seed, backfill embeddings
db/schema.sql           # partitions, indexes, foreign keys, seed dictionaries
document/               # proposal, prototype report, weekly logs, review, delivery docs
start.bat / start.ps1
docker-compose.yml
```

### Main endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/auth/register` · `/api/auth/login` | Register / sign in (Redis-backed tokens) |
| GET | `/api/auth/meta` | Roles and modules |
| GET/POST | `/api/sources` | List / add sources |
| POST | `/api/sources/{id}/trigger` · `/api/sources/trigger-all` | Fetch one / all sources |
| GET | `/api/sources/stats` | Ingestion overview (totals, module/source split, vectors) |
| POST | `/api/sources/purge` | Clear crawled data (admin password required) |
| POST | `/api/reports/generate` | Generate a report (`lang=zh/en`) |
| GET/DELETE | `/api/reports/{id}` | Report detail / delete |
| POST | `/api/chat/ask` | RAG answer with structured citations |
| GET | `/api/system/events` | SSE live event stream |
| GET | `/api/health` | Health check |

### Screenshots

| Sources & ingestion | Report detail | Knowledge chat |
|---|---|---|
| ![](screenshots/01-data-sources-ingestion.png) | ![](screenshots/02-ai-report-detail.png) | ![](screenshots/03-knowledge-chat.png) |

### Documentation

Technical proposal and reflection, prototype report, MVP record, five phase weekly logs, code review log, work-time log, individual reflection and contribution report — all under `document/`.

---

## 中文

### 功能

- **多源采集**：按站点配置解析列表页与正文（新浪财经、第一财经、格隆汇、CNBC、新华网、国家统计局等真实来源），支持 JSON API 源
- **数据入库**：`raw_data` 按月分区，按 `(source_id, content_hash)` 去重，先入库后向量化
- **AI 报告**：按角色（管理者 / 销售 / 技术）与订阅模块生成，输出目录 + 分章节；按模块配额取数，避免高产新闻源挤掉低频统计数据；可溯源到原始文章
- **知识库问答**：pgvector 检索 + Redis 缓存优先 + 短期上下文；回答附带结构化引用（标题、原文链接、命中片段）
- **双语界面**：全站 CN/EN 切换，数据库中的中文数据源名、报表标题、日志事件均有英文映射
- **管理员模式**：输入密码解锁「后端同步状态」，可见事件流、表计数、分区列表、向量数
- **实时管道可视化**：SSE 推送抓取、入库、向量化、报告、对话事件

### 技术栈

| 层 | 选型 |
|---|---|
| 后端 | Python 3.12 · FastAPI · SQLAlchemy 2.0 (async, asyncpg) · Pydantic v2 |
| 数据库 | PostgreSQL 16 + pgvector（月分区、GIN 全文/JSONB 索引、HNSW 向量索引） |
| 缓存 | Redis 7（会话令牌、问答缓存、对话上下文、管理员标记、翻译缓存） |
| 管道 | 进程内 Broker（可切 Kafka）· 进程内调度器（替代 Airflow） |
| 模型 | 对话 DeepSeek `deepseek-chat`（OpenAI 兼容协议）· 向量 `BAAI/bge-m3`（1024 维） |
| 前端 | 单页 HTML + 原生 JS（无构建步骤） |

### 快速开始（Windows）

前置：Windows 10/11 + WSL2 + Docker（容器跑在 WSL 内）+ Python 3.12

```powershell
# 1) 配置后端环境变量
copy backend\.env.example backend\.env
#    填入 OPENAI_API_KEY（对话）与 EMBEDDING_API_KEY（向量）

# 2) 一键启动：拉起容器 → 等健康 → 启动后端 → 打印地址
start.bat
```

启动后：

- 系统入口 <http://127.0.0.1:8000/>
- 介绍页 <http://127.0.0.1:8000/welcome>
- 演示账号 `demo / demo1234`（首次需运行种子脚本）

### 手动启动

```powershell
wsl -e bash -lc "cd /mnt/t/.../CrouchPotatoes && docker compose up -d"   # PostgreSQL + Redis
cd backend
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

初始化数据（可选）：

```powershell
.\.venv\Scripts\python.exe scripts\seed_demo.py          # 演示账号 + 真实数据源
.\.venv\Scripts\python.exe scripts\backfill_embeddings.py # 补历史数据向量
```

> WSL 空闲时会自动关机并连带停掉容器。`start.ps1` 已内置常驻进程与 PostgreSQL/Redis 双健康检查。

### 配置项（`backend/.env`）

| 变量 | 说明 |
|---|---|
| `POSTGRES_DSN` | 默认 `postgresql+asyncpg://domel:domel@localhost:5432/domel` |
| `REDIS_URL` | 默认 `redis://localhost:6379/0` |
| `OPENAI_API_KEY` / `OPENAI_BASE_URL` / `LLM_MODEL` | 对话模型（默认 DeepSeek） |
| `EMBEDDING_API_KEY` / `EMBEDDING_BASE_URL` / `EMBEDDING_MODEL` / `EMBEDDING_DIM` | 向量模型；`EMBEDDING_DIM` 必须与 DDL 中 `vector(N)` 一致 |
| `BROKER_BACKEND` | `memory`（默认）或 `kafka` |
| `SCHEDULER_ENABLED` / `SCHEDULER_INTERVAL_SECONDS` | 进程内定时抓取 |
| `REPORT_LOOKBACK_HOURS` | 报告取数窗口 |
| `VECTOR_BACKEND` | `pgvector` / `qdrant` / `milvus` |
| `ADMIN_PASSWORD` | 管理员模式密码 |

### 目录结构

```
backend/
  app/
    api/          # auth / sources / reports / chat / system / demo 路由
    core/         # 配置、事件总线、缓存、安全、数据源英文名
    db/           # ORM 模型与会话
    pipeline/     # Broker 与调度器
    services/     # 抓取入库、报告、RAG、AI 客户端、向量库
    static/       # 单页前端 + 介绍页 + Logo
  scripts/        # 建库、种子数据、补向量
db/schema.sql     # 分区表 / 索引 / 外键 / 种子字典
document/         # 技术方案、原型报告、周报、评审与交付文档
start.bat / start.ps1
docker-compose.yml
```

### 主要接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/auth/register` `/api/auth/login` | 注册 / 登录（Redis 令牌） |
| GET | `/api/auth/meta` | 角色与模块字典 |
| GET/POST | `/api/sources` | 数据源列表 / 新增 |
| POST | `/api/sources/{id}/trigger` | 抓取单个源 |
| POST | `/api/sources/trigger-all` | 一键抓取全部 |
| GET | `/api/sources/stats` | 入库概览（总量、模块/来源分布、向量数） |
| POST | `/api/sources/purge` | 清空抓取数据（需管理员密码） |
| POST | `/api/reports/generate` | 生成报告（`lang=zh/en`） |
| GET/DELETE | `/api/reports/{id}` | 报告详情 / 删除 |
| POST | `/api/chat/ask` | RAG 问答（返回结构化引用） |
| GET | `/api/system/events` | SSE 实时事件流 |
| GET | `/api/health` | 健康检查 |

### 界面截图

| 数据源与入库 | 报告详情 | 知识库对话 |
|---|---|---|
| ![](screenshots/01-data-sources-ingestion.png) | ![](screenshots/02-ai-report-detail.png) | ![](screenshots/03-knowledge-chat.png) |

### 交付文档

技术方案与开发反思、技术原型报告、MVP 开发记录、5 份阶段周报、代码评审日志、工作时间日志、个人反思与贡献报告，均在 `document/` 内。

---

**Team** CrouchPotatoes ｜ **Repository** <https://github.com/luotingzhu/CrouchPotatoes>
