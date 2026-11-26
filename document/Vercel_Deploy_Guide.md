# 部署到 Vercel + Neon + Upstash

本项目原始形态是「本地 FastAPI 常驻进程 + Docker 里的 PostgreSQL/Redis」，Vercel 是无服务器平台，
因此代码里已做了一组适配（见文末「已为你改好的部分」）。按下面的顺序操作即可上线。

> 预计耗时：Neon 建库 5 分钟、Upstash 3 分钟、Vercel 部署 10 分钟、验证 5 分钟。

---

## 0. 前置准备

- GitHub 仓库已推送（<https://github.com/luotingzhu/CrouchPotatoes>）
- 本机保留 `backend/.venv`（仅用于向云数据库灌入表结构与种子数据）
- 账号：Vercel、Neon、Upstash（都可用 GitHub 登录，免费额度足够课程演示）

---

## 1. Neon：PostgreSQL + pgvector

1. 打开 <https://console.neon.tech> → **Create project**（区域选 Singapore，离国内近）
2. 在 **Dashboard → Connection string** 里复制连接串，形如：
   ```
   postgresql://neondb_owner:xxxx@ep-cool-name-123456-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require
   ```
   **要选带 `-pooler` 的那条**（连接池端点，Serverless 下更稳）。

3. 转成后端需要的 DSN（加 `+asyncpg`、`sslmode` 改成 `ssl`）：
   ```
   postgresql+asyncpg://neondb_owner:xxxx@ep-cool-name-123456-pooler.ap-southeast-1.aws.neon.tech/neondb?ssl=require
   ```
   这个值后面填到 Vercel 的 `POSTGRES_DSN`。

4. 在本机建表（schema 里有 `CREATE EXTENSION vector`，Neon 支持 pgvector）：

   ```powershell
   cd backend
   $env:POSTGRES_DSN = "postgresql+asyncpg://...?ssl=require"
   .\.venv\Scripts\python.exe scripts\init_db.py
   ```

5. 灌入演示账号与真实数据源（同样指定云库）：

   ```powershell
   $env:POSTGRES_DSN = "postgresql+asyncpg://...?ssl=require"
   .\.venv\Scripts\python.exe scripts\seed_demo.py
   ```

   完成后再设置 `REDIS_URL`（下一步）跑一次抓取，数据就会进入 Neon。

---

## 2. Upstash：Redis

1. 打开 <https://console.upstash.com> → **Create Database**，区域选 `ap-southeast-1`，类型 **Regional**
2. 在详情页复制 **Redis URL**，形如：
   ```
   rediss://default:AXxxxx@eu1-xxxx.upstash.io:6379
   ```
   Upstash 默认要求 TLS，所以前缀是 `rediss://`，后端 `redis-py` 原生支持。
3. 这个值填到 Vercel 的 `REDIS_URL`。

---

## 3. Vercel：导入仓库

1. 打开 <https://vercel.com/new> → 导入 `luotingzhu/CrouchPotatoes`
2. **Framework Preset** 选 `Other`，Build / Output 留空（使用仓库里的 `vercel.json`）
3. 在 **Environment Variables** 中填入（Production + Preview 都勾选）：

| 变量 | 值 |
|---|---|
| `POSTGRES_DSN` | 第 1 步转好的 `postgresql+asyncpg://...?ssl=require` |
| `REDIS_URL` | 第 2 步的 `rediss://default:...` |
| `OPENAI_API_KEY` | DeepSeek 的 key |
| `OPENAI_BASE_URL` | `https://api.deepseek.com/v1` |
| `LLM_MODEL` | `deepseek-chat` |
| `EMBEDDING_API_KEY` | SiliconFlow 的 key |
| `EMBEDDING_BASE_URL` | `https://api.siliconflow.cn/v1` |
| `EMBEDDING_MODEL` | `BAAI/bge-m3` |
| `EMBEDDING_DIM` | `1024`（必须与 `db/schema.sql` 的 `vector(1024)` 一致） |
| `VECTOR_BACKEND` | `pgvector` |
| `BROKER_BACKEND` | `memory` |
| `SCHEDULER_ENABLED` | `false` |
| `SERVERLESS` | `true` ← 关键开关 |
| `REPORT_LOOKBACK_HOURS` | `168`（7 天，数据少时不至于报“过时”） |
| `ADMIN_PASSWORD` | 你的管理员密码 |

4. 点 **Deploy**，等待构建完成（首次约 3–5 分钟）。

---

## 4. 验证上线

把 `<your-app>.vercel.app` 替换成实际域名：

```powershell
# 健康检查：应返回 serverless=true
curl.exe -s https://<your-app>.vercel.app/api/health

# 登录拿 token
curl.exe -s -X POST https://<your-app>.vercel.app/api/auth/login `
  -H "Content-Type: application/json" -d "{\"username\":\"demo\",\"password\":\"demo1234\"}"
```

浏览器打开 `https://<your-app>.vercel.app/`：

- 用 `demo / demo1234` 登录
- 「数据源与入库」→ 入库概览应有数据（来自 Neon）
- 点单个源的「抓取」→ 等待 20–40 秒，应看到 `raw_data` 条数增加（同步入库，无队列）
- 「AI 情报报告」→ 生成报告（LLM 调用 10–30 秒）
- 「知识库对话」→ 提问，回答下方带「参考来源」链接

---

## 5. 与本地部署的差异（务必知晓）

| 项目 | 本地 | Vercel |
|---|---|---|
| 抓取入库 | 投递内存队列 → 常驻消费者异步入库 | **请求内同步完成**（抓取 → 入库 → 向量化） |
| 定时抓取 | 可开 `SCHEDULER_ENABLED` | **不可用**，需手动点抓取或配 Vercel Cron 调 `/api/sources/trigger-all` |
| 实时事件 | SSE 长连接 | **改为 3 秒轮询** `/api/system/events/recent` |
| 单次执行时长 | 无限制 | **60 秒上限**（Hobby），`trigger-all` 到 40 秒会自动停止并提示再点一次 |
| 数据库连接 | 本地 PG，连接池 10 | Neon 连接池 + 连接数上限，池已自动压到 2 |
| 冷启动 | 无 | 首次访问约 1–3 秒（依赖较重） |

补充说明：

- **一键抓取全部**在 Serverless 下会按时间预算处理，剩余源需要再点一次；演示时建议只抓 1–2 个源。
- **报告生成**如果模型响应慢，可能触及 60 秒上限；可以换更快的模型，或把 `REPORT_LOOKBACK_HOURS` 调小减少上下文。
- **SSE 面板**在 Vercel 上显示的是轮询到的事件，最多 20 条，属于预期行为。
- `document/`、`screenshots/`、`db/` 已在 `.vercelignore` 中排除，不会进入函数包。

---

## 6. 已为你改好的部分（代码层面）

| 文件 | 改动 |
|---|---|
| `api/index.py` | Vercel 入口，把 `backend` 加入 `sys.path` 并导出 FastAPI `app` |
| `vercel.json` | Python 3.12 函数、`maxDuration=60`、静态文件打包、全路径重写到函数 |
| `requirements.txt`（根目录） | Serverless 所需依赖（去掉 uvicorn / aiokafka / alembic / psycopg） |
| `.vercelignore` | 排除虚拟环境、文档、截图、DDL 等非运行时文件 |
| `app/core/config.py` | 新增 `SERVERLESS` 开关 |
| `app/db/session.py` | 识别 Neon/PgBouncer，关闭 `statement_cache` 并压缩连接池 |
| `app/main.py` | Serverless 下不启动 Broker 消费者与调度器；`/api/health` 返回 `serverless` 标记 |
| `app/services/ingestion_service.py` | 新增 `run_source_inline()`：抓取后逐条入库并向量化 |
| `app/api/sources.py` | `trigger` / `trigger-all` 在 Serverless 下走同步入库，并按 40 秒预算截断 |
| `app/api/system.py` | 新增 `GET /api/system/events/recent` 供轮询 |
| `app/static/index.html` | 前端读取 `/api/health`，Serverless 时自动切换为 3 秒轮询 |

本地验证方式（不部署也能验证这些改动）：

```powershell
cd backend
$env:SERVERLESS = "true"
.\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8011
```

`/api/health` 返回 `serverless: true`，抓取为同步入库，前端事件走轮询——与 Vercel 上行为一致。
