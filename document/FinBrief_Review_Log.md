# FinBrief 软件审阅日志

> 用途：供其他 AI / 评审人快速建立对 FinBrief 的完整认知，并与同事的 PPT 逐项比对，产出优化建议。
> 记录时间：2026-11-13（以当前代码与本地实测为准）
> 阅读顺序建议：第 1 章 → 第 3 章 → 第 6 章 → 第 10 章（比对清单）→ 第 11 章（优化点）

---

## 1. 一句话定位

**FinBrief 是一套面向电机制造企业的 B 端市场情报工作台**：把新闻、政策、商业统计、金融四类碎片信息自动聚合入库，按用户角色与订阅模块生成可溯源的中文/英文情报报告，并支持基于知识库的多轮追问。

- 产品形态：Web 应用（单页 SPA + FastAPI 后端），本地部署，数据留在自有数据库。
- 目标用户：企业管理者（决策摘要）、销售（客户与竞品动态）、技术（政策与供应链）。
- 解决的问题：市场数据分散在多个站点、人工检索耗时、报告无法溯源、追问需要重新查资料。

---

## 2. 核心用户流程

```
注册 / 登录
   │  选择角色（管理者 / 销售 / 技术）+ 勾选情报模块（新闻 / 政治 / 商业统计 / 金融）
   ▼
数据源与入库 ── 一键抓取全部 ──> 抓取 -> 队列 -> 去重 -> 分区表 -> 向量化
   ▼
AI 情报报告 ── 生成新报告 ──> 按订阅模块取近 72h 数据 -> 三段合成 -> 落库 + 溯源 + 向量化
   ▼
知识库对话 ── 提问 ──> 查询向量化 -> pgvector 检索 Top-6 -> LLM 带引用回答 -> Redis 缓存
   ▼
（管理员）后端同步状态 ──> 表计数 / 分区 / 向量数 / SSE 事件流
```

---

## 3. 功能清单与实际状态

| 功能 | 状态 | 说明 |
|---|---|---|
| 注册 / 登录 / 登出 | 已实现 | PBKDF2-SHA256 密码哈希；会话令牌存 Redis；邮箱格式校验；中英文错误提示 |
| 角色与模块配置 | 已实现 | users / roles / user_roles / modules / user_module_prefs 五张表；注册时写入，配置页可改 |
| 数据源管理 | 已实现 | 新增源（api / rss / crawler）、启用停用、单源触发、一键全部触发 |
| 多源抓取 | 已实现 | httpx 抓取 + BeautifulSoup 解析正文；内置 4 个演示源 + 8 个真实财经站点 |
| 去重 | 已实现 | 按内容哈希去重；同一天重复抓取不会重复入库 |
| 分区存储 | 已实现 | raw_data 按 publish_time 月分区；chat_messages 按 created_at 月分区 |
| 向量化 | 已实现 | SiliconFlow `BAAI/bge-m3`（1024 维）→ pgvector；HNSW 余弦索引 |
| AI 报告生成 | 已实现 | DeepSeek 生成「高亮摘要 / 经济分析 / 国家政策」三段；写入 report_sections |
| 报告溯源 | 已实现 | report_sources 多对多关联原始数据，前端展示标题与链接 |
| 报告目录与分章节 | 已实现 | 前端生成目录 + 锚点跳转 + 01/02/03 章节版式 |
| 数据新鲜度提示 | 已实现 | 素材全部早于时间窗时提示「仅有旧数据」，并列出涉及模块与可订阅模块 |
| 报告翻译 | 已实现 | `POST /api/reports/{id}/translate`，生成英文版本 |
| RAG 多轮对话 | 已实现 | 检索 Top-6 片段，回答带 `[n]` 引用；会话短期上下文存 Redis |
| 问答缓存 | 已实现 | Redis cache-aside，重复问题直接命中 |
| 中英文切换 | 已实现 | 界面文案全量双语；新闻/报告正文属数据，不随界面切换 |
| 管理员模式 | 已实现 | 配置页输入管理员密码解锁「后端同步状态」，解锁态存 Redis（默认 8 小时） |
| 实时管道可视化 | 已实现 | SSE `/api/system/events`；底部状态条滚动展示抓取/入库/向量化事件 |
| 后端同步面板 | 已实现 | 表计数、分区列表、向量数、事件流（仅管理员） |
| 定时调度 | 未启用 | 内置调度器存在但 `SCHEDULER_ENABLED=false`，当前依赖手动/一键抓取 |
| 自动归档 | 已定义未调度 | `archive_old_partitions()` 存在，未挂定时任务 |

---

## 4. 系统架构

### 4.1 分层

```
┌──────────────────────────────────────────────────────────────┐
│ 前端（单文件 SPA，原生 JS）                                    │
│  index.html：登录 / 数据源 / 报告 / 对话 / 配置 / 同步状态      │
│  landing.html：公开介绍页（/welcome）                          │
└───────────────┬──────────────────────────────────────────────┘
                │ REST + SSE
┌───────────────▼──────────────────────────────────────────────┐
│ API 层（FastAPI，app/api/*.py）                               │
│  auth · prefs · sources · reports · chat · system · demo      │
└───────────────┬──────────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────────┐
│ 服务层（app/services/*.py）                                    │
│  IngestionService · ReportService · RagService                 │
│  AiClient（对话 / Embedding） · VectorStore（pgvector/Qdrant）  │
└───────┬──────────────────┬──────────────────┬────────────────┘
        │                  │                  │
   PostgreSQL 16       Redis 7          Broker（内存队列）
   + pgvector         会话 / 上下文 /     可切 Kafka
   + pg_trgm          问答缓存
   + pgcrypto
```

### 4.2 技术选型

| 层 | 选型 | 备注 |
|---|---|---|
| Web 框架 | FastAPI + Uvicorn | 全异步 |
| ORM | SQLAlchemy 2.0 async + asyncpg | 类型标注 `Mapped` |
| 主库 | PostgreSQL 16 + pgvector / pg_trgm / pgcrypto | 业务 + 向量 + 全文检索同库 |
| 缓存 | Redis 7 | 会话、对话上下文、问答缓存、管理员解锁态 |
| 消息 | 内存 asyncio 队列（默认）/ Kafka（可切换） | `BROKER_BACKEND=memory` |
| 调度 | 进程内调度器（默认关闭） | 设计上可替换 Airflow |
| 对话模型 | DeepSeek `deepseek-chat` | OpenAI 兼容协议 |
| 向量模型 | SiliconFlow `BAAI/bge-m3`（1024 维） | 与对话模型分离配置 |
| 前端 | 原生 HTML/CSS/JS，单文件 | 无构建步骤、无框架依赖 |

> 关键事实：需求文档中的 **Kafka + Airflow + Milvus/Qdrant** 属于「可选/设计态」，演示环境实际运行的是 **内存队列 + 关闭调度 + pgvector**。任何 PPT 若把前四者画成已部署组件，需要与代码对齐口径。

---

## 5. 数据架构

### 5.1 实体关系（概要）

```
users ──< user_roles >── roles
  │
  ├──< user_module_prefs >── modules
  │
  ├──< reports ──< report_sections
  │        └────< report_sources >──── raw_data ──< data_sources
  │
  └──< chat_sessions ──< chat_messages

embeddings（多态：owner_type = report_section | raw_data）
```

### 5.2 表清单

| 分组 | 表 |
|---|---|
| 用户与权限 | users, roles, user_roles |
| 个性化 | modules, user_module_prefs |
| 数据源与原始数据 | data_sources, raw_data（月分区） |
| AI 报告 | reports, report_sections, report_sources |
| AI 对话 | chat_sessions, chat_messages（月分区） |
| 向量 | embeddings |

数据库当前共 21 个表对象（含分区子表与 default 兜底分区）。

### 5.3 物理设计要点

| 设计 | 实现 |
|---|---|
| 主键 | UUID（`gen_random_uuid()`）；分区表主键含分区键（如 `(data_id, publish_time)`） |
| 时间 | `TIMESTAMP WITH TIME ZONE` |
| 长文本 | `TEXT` |
| 结构化扩展 | `metadata JSONB` |
| 分区 | `raw_data` 按 `publish_time` 月 RANGE；`chat_messages` 按 `created_at` 月 RANGE；`ensure_monthly_partition()` 幂等预建 |
| 索引 | 外键/时间 B-Tree；`raw_data.content`、`report_sections.content` GIN 全文；`metadata` GIN（`jsonb_path_ops`）；`embeddings` HNSW（`vector_cosine_ops`） |
| 生命周期 | 近 3 个月热数据；`archive_old_partitions()` 用 DETACH 移入 `archive` schema，避免大表 DELETE |
| 向量 | `vector(1024)`，与 `EMBEDDING_DIM` 强绑定 |

> 已知取舍：中文全文检索当前使用 `simple` 配置（无中文分词器），靠 `pg_trgm` 部分补偿。生产环境应引入 `zhparser` 或 `pg_jieba`。

---

## 6. AI 数据管道（核心链路）

### 6.1 抓取入库

```
调度/手动触发
   │
   ▼
IngestionService.run_source(source_id)
   │  按 source_type 分派：api / rss / crawler
   │  crawler：httpx 拉列表页 -> BeautifulSoup 提取链接 -> 抓正文 -> 清洗
   ▼
Broker.publish(topic=raw_data)      ← 解耦、削峰、可重放
   │
   ▼
consume_loop()                      ← 常驻消费者
   │  1) 按 (source_id, content_hash) 去重
   │  2) 命中重复 -> 跳过
   │  3) 新数据 -> 写入 raw_data 分区表
   │  4) 事务提交后才做 Embedding（外部调用不占库事务）
   ▼
VectorStore.upsert(raw_data chunks) → pgvector
```

设计要点：
- **抓取与入库解耦**：HTTP 抓取失败不会污染入库事务。
- **手动提交 offset**：消费者处理成功才确认，保证不丢（内存队列下为语义模拟）。
- **先提交后向量化**：避免 LLM/Embedding 网络抖动导致长事务。
- **按天快照**：内置演示源内容附带当日日期，保证每天可产生新数据；同一天重复抓取仍去重。

### 6.2 报告生成

```
POST /api/reports/generate?lang=zh
   │
   ▼
读取用户角色 + 启用的订阅模块
   │
   ▼
取近 N 小时（默认 72h）raw_data
   │  为空 -> 回退到该用户订阅模块的历史最新数据（最多 50 条）
   │  无任何订阅 -> 400 明确提示
   ▼
按模块分组拼装上下文（控制 token）
   │
   ▼
LLM 生成三段：高亮摘要 / 经济分析 / 国家政策
   │
   ▼
事务内写入 reports + report_sections + report_sources
   │
   ▼
对章节内容切块向量化 -> pgvector
   │
   ▼
返回 ReportOut（含 sections、sources、data_stale、模块提示）
```

- 生成是**同步请求**，单次约 20–40 秒（取决于模型与素材量）。
- 报告只使用**用户订阅模块**的数据；这是产品口径，不是缺陷。
- `data_stale=true` 时前端提示「仅有旧数据」，并列出「涉及模块」与「当前有新数据但未订阅的模块」。

### 6.3 RAG 对话

```
POST /api/chat/ask
   │
   ├─ 1) Redis 查缓存（问题 + 用户 + 语言）
   │      命中 -> 直接返回（cache_hit=true）
   │
   ├─ 2) 未命中 -> Embedding(question)
   │
   ├─ 3) pgvector 检索 Top-6（owner_type: report_section / raw_data）
   │
   ├─ 4) 取 Redis 中的短期对话上下文（默认 1 小时）
   │
   ├─ 5) LLM 生成带 [n] 引用的回答
   │
   └─ 6) 写 chat_messages + 更新上下文 + 回写缓存（默认 30 分钟）
```

---

## 7. 接口清单

| 模块 | 方法与路径 |
|---|---|
| 认证 | `GET /api/auth/meta`、`POST /api/auth/register`、`POST /api/auth/login`、`POST /api/auth/logout`、`GET /api/auth/me` |
| 偏好 | `GET /api/prefs`、`PUT /api/prefs` |
| 数据源 | `GET/POST /api/sources`、`POST /api/sources/{id}/trigger`、`POST /api/sources/trigger-all`、`GET /api/sources/raw` |
| 报告 | `POST /api/reports/generate`、`GET /api/reports`、`GET /api/reports/{id}`、`POST /api/reports/{id}/translate` |
| 对话 | `GET/POST /api/chat/sessions`、`GET /api/chat/sessions/{id}/messages`、`POST /api/chat/ask` |
| 系统 | `GET /api/system/status`（管理员）、`GET /api/system/events`（SSE）、`GET/POST /api/system/admin/status|unlock|lock` |
| 演示 | `GET /api/demo/feed/{module_code}`（内置源） |
| 健康 | `GET /api/health`（公开） |
| 页面 | `GET /`（应用）、`GET /welcome`（介绍页） |

---

## 8. 前端

- 技术：单文件 `index.html`，原生 JS，无构建、无 CDN 依赖，可离线打开。
- 页面（左侧导航）：
  1. 数据源与入库
  2. AI 情报报告
  3. 知识库对话
  4. 个性化配置
  5. 后端同步状态（仅管理员模式解锁后可见）
- 全局能力：CN/EN 切换（162 条词条）、SSE 实时事件条、Toast、统一错误解析（兼容 FastAPI 422 数组）。
- 介绍页：`landing.html`，公开访问 `/welcome`，用于对外展示与答辩开场。
- 品牌：FinBrief；Logo 位于 `app/static/logo.png`（浅色场景）与 `logo_trans.png`（透明底）。

---

## 9. 部署与运行

```bash
# 1) 数据库与缓存（WSL / Docker）
docker compose up -d            # pgvector/pgvector:pg16 + redis:7-alpine

# 2) 后端
cd backend
pip install -r requirements.txt
cp .env.example .env            # 填 OPENAI_API_KEY / EMBEDDING_API_KEY
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

关键环境变量（`.env`）：

| 变量 | 作用 |
|---|---|
| `POSTGRES_DSN` / `REDIS_URL` | 主库与缓存连接 |
| `BROKER_BACKEND` | `memory` / `kafka` |
| `SCHEDULER_ENABLED` | 是否启用进程内定时抓取 |
| `VECTOR_BACKEND` | `pgvector` / `qdrant` / `milvus` |
| `OPENAI_*` | 对话模型（DeepSeek 等兼容网关） |
| `EMBEDDING_*` | 向量模型（可与对话分离） |
| `EMBEDDING_DIM` | 必须与 `vector(N)` 一致 |
| `REPORT_LOOKBACK_HOURS` | 报告时间窗（当前 72） |
| `ADMIN_PASSWORD` | 管理员模式密码（勿写入前端/文档） |

初始化：`db/schema.sql` 建表与分区；`backend/scripts/seed_demo.py` 写入角色、模块、演示源与账号；`scripts/backfill_embeddings.py` 用于换向量模型后补历史数据。

---

## 10. 与 PPT 比对工作表

> 用法：逐行核对 PPT 的表述与「代码事实」。不一致时优先改 PPT 口径，或列入优化项。

| # | PPT 常见主题 | 代码事实 | 若不一致的建议 |
|---|---|---|---|
| 1 | 产品定位 | 电机行业 B 端情报工作台，强调可溯源与多轮追问 | 若 PPT 写成通用「舆情系统」，补充行业垂直与溯源差异 |
| 2 | 用户流程 | 注册 → 选角色/模块 → 抓取 → 报告 → 对话 | 若 PPT 缺少「订阅模块」环节，补上，这是报告口径的关键 |
| 3 | 架构图 | FastAPI + PostgreSQL(pgvector) + Redis + 可选 Kafka/Airflow | 若把 Kafka/Airflow/Milvus 画成已部署，改为「可选/设计态」，或补实现 |
| 4 | 数据模型 | 13 张核心表；raw_data/chat_messages 月分区 | 若 PPT 只画 ER 不画分区，建议补一张「分区与索引」示意 |
| 5 | 索引策略 | B-Tree + GIN 全文 + JSONB GIN + HNSW | 若 PPT 只写「建了索引」，补充四类索引的用途 |
| 6 | 抓取链路 | 抓取 → 内存队列 → 去重 → 入库 → 向量化 | 若 PPT 画成「爬虫直接写库」，与代码不符 |
| 7 | AI 报告 | 三段式（摘要/经济/政策）+ 溯源 + 目录 | 若 PPT 展示报告截图无目录/溯源，建议更新为当前版式 |
| 8 | RAG | Top-6 检索 + 引用 + Redis 缓存 + 短期上下文 | 若 PPT 只写「接入大模型」，补充检索与缓存细节 |
| 9 | 模型选型 | 对话 DeepSeek；向量 BGE-M3（1024 维） | 若 PPT 写「单一 OpenAI」，改为双服务商并说明原因 |
| 10 | 非功能 | 分区、冷热归档、GIN、HNSW、缓存 | 若 PPT 未提，建议补「性能与生命周期」一页 |
| 11 | 演示路径 | 抓取 → 报告 → 对话 → 管理员同步面板 | 若 PPT 无「后端同步可视化」，这是现场加分项，建议加入 |
| 12 | 安全 | PBKDF2、Redis 会话、管理员密码门 | 若 PPT 宣称 RBAC/审计，当前未实现，需降级表述 |
| 13 | 规模与指标 | 12 个数据源、4 个模块、单次抓取数十条 | 若 PPT 写「百万级数据」，需标注为设计目标而非实测 |
| 14 | 部署 | 本地 Docker + Uvicorn；可内网穿透演示 | 若 PPT 写「云原生/K8s」，需说明未落地 |

---

## 11. 已知限制与技术债（优化点来源）

### P0（答辩/上线前必须处理）

1. **口径一致性**：PPT 与代码对 Kafka / Airflow / Milvus 的表述必须统一，避免被问「跑起来了吗」。
2. **凭据管理**：`.env` 明文密钥；登录页预填演示账号；公网穿透前必须改密码、换 Key、关闭默认账号预填。
3. **数据库迁移**：当前只有 `db/schema.sql`，无 Alembic。任何表结构变更都需手工执行，建议引入 Alembic。
4. **自动化测试**：无单元/集成测试。至少补：认证、抓取去重、报告生成（mock LLM）、RAG 检索（mock Embedding）。

### P1（明显影响体验/可维护性）

5. **报告同步生成**：单次 20–40 秒，公网/代理易超时。建议改为后台任务 + 轮询/SSE 进度。
6. **正文 Markdown 未渲染**：报告中的表格、加粗以纯文本显示；目录只到章节级。建议接入轻量 Markdown 渲染，并把二级标题纳入目录。
7. **政治/商业统计缺真实源**：目前依赖内置演示源；建议接入统计公报/政策发布类站点，并补模块分类器。
8. **中文全文检索**：`simple` 配置无中文分词，召回有限；建议 `zhparser`/`pg_jieba` + 重建 GIN 索引。
9. **管理员权限模型**：共享密码 + Redis 标记，无角色校验与审计。建议改为 `users.is_admin` + 操作日志。
10. **抓取健壮性**：站点改版会导致选择器失效；建议增加解析失败告警与源健康度指标。

### P2（锦上添花）

11. **可观测性**：结构化日志、请求耗时、抓取成功率、Token 消耗统计。
12. **导出能力**：报告导出 PDF/DOCX，便于汇报与归档。
13. **归档调度**：把 `archive_old_partitions()` 挂到定时任务，真正实现冷热分离。
14. **分页与检索**：报告列表、原始数据列表增加分页与关键词筛选。
15. **限流与配额**：对外暴露时增加 IP/用户级限流，防止 API Key 被刷。

---

## 12. 文件地图（便于审阅定位）

```
CrouchPotatoes/
├─ db/schema.sql                      # DDL：表、分区、索引、归档函数
├─ docker-compose.yml                 # pgvector + redis
├─ backend/
│  ├─ requirements.txt
│  ├─ .env / .env.example             # 连接与模型配置（勿外传）
│  ├─ app/
│  │  ├─ main.py                      # FastAPI 入口、路由装配、/ 与 /welcome
│  │  ├─ schemas.py                   # Pydantic 出入参
│  │  ├─ core/{config,security,cache,events}.py
│  │  ├─ db/{base,models,session}.py  # ORM 模型
│  │  ├─ api/{auth,prefs,sources,reports,chat,system,demo}.py
│  │  ├─ services/
│  │  │  ├─ ingestion_service.py      # 抓取 + 入库 + 去重 + 向量化
│  │  │  ├─ report_service.py         # 报告合成 + 溯源落库
│  │  │  ├─ rag_service.py            # 检索 + 缓存 + 多轮
│  │  │  ├─ vector_store.py           # pgvector / Qdrant 抽象
│  │  │  └─ ai_client.py              # 对话 / Embedding 客户端
│  │  ├─ pipeline/{broker,scheduler}.py
│  │  └─ static/{index.html,landing.html,logo*.png}
│  └─ scripts/{seed_demo.py,backfill_embeddings.py}
└─ document/
   ├─ FinBrief_Review_Log.md          # 本文档
   └─ Weekly_Log_*.txt / 技术方案与开发反思.md 等历史记录
```

---

## 13. 给审阅 AI 的直接提示词

> 你可以把下面这段连同本文档一起交给评审 AI：

```
你是资深 B 端产品与技术评审专家。请阅读《FinBrief 软件审阅日志》，并与我提供的 PPT 内容逐项比对：
1) 找出 PPT 与代码事实不一致的地方（重点：架构组件是否真实部署、AI 链路、数据模型、非功能设计）；
2) 指出 PPT 缺失但代码已实现、且值得展示的亮点；
3) 指出 PPT 夸大或无法被代码支撑的表述；
4) 按 P0/P1/P2 给出优化建议，每条包含：问题、影响、具体改法、可验证的验收标准；
5) 最后给出 3 个答辩时最可能被追问的问题及建议回答要点。
输出用中文，结构化、可执行，不要泛泛而谈。
```
