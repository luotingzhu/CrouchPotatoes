-- =============================================================================
-- Domel 电机市场情报 AI 助手 · PostgreSQL 物理模型 DDL
-- 目标版本 : PostgreSQL 15+
-- 依赖扩展 : pgcrypto(gen_random_uuid) / pg_trgm(模糊匹配) / vector(pgvector)
-- 关键设计 :
--   1) 主键默认使用 UUID(gen_random_uuid)；如需雪花算法 BIGINT，见文件末尾方案说明
--   2) 长文本统一 TEXT；时间统一 TIMESTAMPTZ
--   3) 外键 + 时间字段使用 B-Tree 索引；metadata JSONB 使用 GIN 索引
--   4) raw_data.content / report_sections.content 使用 GIN 全文检索索引(+trigram)
--   5) raw_data 按 publish_time、chat_messages 按 created_at 做月分区；冷热分离
-- 注意 :
--   分区表的 PRIMARY KEY / UNIQUE 约束必须包含分区键，因此 raw_data 的主键为
--   (data_id, publish_time)，chat_messages 的主键为 (message_id, created_at)。
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. 扩展
-- -----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- 提供 gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- 提供 gin_trgm_ops，中文/模糊匹配友好
CREATE EXTENSION IF NOT EXISTS vector;    -- pgvector，向量检索

-- 全文检索配置：
--   英文环境可用 'english'；中文生产环境建议安装 zhparser / pg_jieba 后统一改为 'chinese'。
--   此处使用 'simple' 以保证在未安装中文分词器时也能正常建表。
--   （下文所有 to_tsvector 均显式指定 ::regconfig 以命中表达式索引）

-- -----------------------------------------------------------------------------
-- 1. 公共触发器函数：自动维护 updated_at
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- 2. 用户与权限
-- =============================================================================

CREATE TABLE users (
    user_id       UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    username      VARCHAR(64)  NOT NULL,
    email         VARCHAR(255) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    status        SMALLINT     NOT NULL DEFAULT 1,          -- 1 启用 / 0 禁用
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uq_users_username UNIQUE (username),
    CONSTRAINT uq_users_email    UNIQUE (email)
);
COMMENT ON TABLE users IS '用户账号表';

CREATE TABLE roles (
    role_id   BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    role_name VARCHAR(32) NOT NULL,        -- Manager / Sales / Tech
    CONSTRAINT uq_roles_name UNIQUE (role_name)
);
COMMENT ON TABLE roles IS '角色字典表（管理者/销售/技术）';

-- 用户-角色 关联表（M:N）
CREATE TABLE user_roles (
    user_id    UUID        NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    role_id    BIGINT      NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, role_id)
);
COMMENT ON TABLE user_roles IS '用户-角色关联表';


-- =============================================================================
-- 3. 个性化配置
-- =============================================================================

CREATE TABLE modules (
    module_id   BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    module_code VARCHAR(32) NOT NULL,      -- news / politics / business / finance
    module_name VARCHAR(64) NOT NULL,      -- 新闻 / 政治 / 商业统计 / 金融
    CONSTRAINT uq_modules_code UNIQUE (module_code)
);
COMMENT ON TABLE modules IS '情报模块字典表';

-- 用户-模块偏好 关联表（M:N，个性化核心）
CREATE TABLE user_module_prefs (
    user_id    UUID        NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    module_id  BIGINT      NOT NULL REFERENCES modules(module_id) ON DELETE CASCADE,
    is_enabled BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, module_id)
);
COMMENT ON TABLE user_module_prefs IS '用户订阅的情报模块（个性化配置）';


-- =============================================================================
-- 4. 数据源与原始数据
-- =============================================================================

CREATE TABLE data_sources (
    source_id   BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_name VARCHAR(128) NOT NULL,
    url         TEXT         NOT NULL,
    source_type VARCHAR(16)  NOT NULL,     -- crawler / api
    status      SMALLINT     NOT NULL DEFAULT 1,   -- 1 启用 / 0 停用
    crawl_cron  VARCHAR(64),               -- 供 Airflow 生成调度使用（如 "0 */2 * * *"）
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT ck_data_sources_type CHECK (source_type IN ('crawler', 'api'))
);
COMMENT ON TABLE data_sources IS '数据源配置表（爬虫/API）';

-- 原始数据：按 publish_time 做月度 RANGE 分区
CREATE TABLE raw_data (
    data_id      UUID        NOT NULL DEFAULT gen_random_uuid(),
    source_id    BIGINT      NOT NULL REFERENCES data_sources(source_id) ON DELETE RESTRICT,
    module_id    BIGINT      NOT NULL REFERENCES modules(module_id)      ON DELETE RESTRICT,
    title        TEXT        NOT NULL,
    content      TEXT,
    url          TEXT,
    publish_time TIMESTAMPTZ NOT NULL,                 -- 分区键
    metadata     JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- 作者/语言/标签/情感等
    content_hash VARCHAR(64),                          -- 去重指纹（sha256）
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- 分区键必须包含在主键中
    CONSTRAINT pk_raw_data PRIMARY KEY (data_id, publish_time)
) PARTITION BY RANGE (publish_time);
COMMENT ON TABLE raw_data IS '原始数据表（按月分区）';

-- 分区管理函数：幂等地为指定父表创建“某月”分区
CREATE OR REPLACE FUNCTION ensure_monthly_partition(
    p_parent_table regclass,
    p_month        date
) RETURNS void AS $$
DECLARE
    v_start date := date_trunc('month', p_month)::date;   -- 分区起点（含）
    v_end   date := (date_trunc('month', p_month) + interval '1 month')::date; -- 分区终点（不含）
    v_name  text := format('%s_%s', p_parent_table::text, to_char(v_start, 'YYYY_MM'));
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = v_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
            v_name, p_parent_table, v_start, v_end
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 兜底分区：捕获超出已建范围的数据，避免插入失败
CREATE TABLE IF NOT EXISTS raw_data_default PARTITION OF raw_data DEFAULT;

-- 预建 raw_data 当月 及 未来 2 个月 的分区
-- （chat_messages 的分区在其建表后另行预建，见第 6 节）
DO $$
DECLARE d date;
BEGIN
    FOR d IN
        SELECT generate_series(
                   date_trunc('month', now())::date,
                   (date_trunc('month', now()) + interval '2 month')::date,
                   interval '1 month'
               )::date
    LOOP
        PERFORM ensure_monthly_partition('raw_data', d);
    END LOOP;
END $$;


-- =============================================================================
-- 5. AI 报告
-- =============================================================================

CREATE TABLE reports (
    report_id      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        UUID        NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    title          TEXT        NOT NULL,
    status         VARCHAR(16) NOT NULL DEFAULT 'generated',  -- generated / failed
    generated_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_reports_status CHECK (status IN ('pending', 'generated', 'failed'))
);
COMMENT ON TABLE reports IS 'AI 生成的定制化情报报告';

CREATE TABLE report_sections (
    section_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    report_id    UUID        NOT NULL REFERENCES reports(report_id) ON DELETE CASCADE,
    section_type VARCHAR(32) NOT NULL,     -- summary / economic / policy
    content      TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_report_sections_type
        CHECK (section_type IN ('summary', 'economic', 'policy'))
);
COMMENT ON TABLE report_sections IS '报告段落（摘要/经济分析/政策）';

-- 报告 <- 溯源 -> 原始数据（M:N）
-- 注意：被引用的 raw_data 为分区表，其主键为 (data_id, publish_time)，
--       因此此处需同时携带 data_publish_time 以建立复合外键（PG12+ 支持引用分区表）。
CREATE TABLE report_sources (
    report_id          UUID        NOT NULL REFERENCES reports(report_id) ON DELETE CASCADE,
    data_id            UUID        NOT NULL,
    data_publish_time  TIMESTAMPTZ NOT NULL,
    relevance          REAL        NOT NULL DEFAULT 1.0,   -- 溯源相关度
    snippet            TEXT,                               -- 引用片段
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (report_id, data_id),
    CONSTRAINT fk_report_sources_raw_data
        FOREIGN KEY (data_id, data_publish_time)
        REFERENCES raw_data (data_id, publish_time) ON DELETE CASCADE
);
COMMENT ON TABLE report_sources IS '报告-原始数据溯源关联表';


-- =============================================================================
-- 6. AI 对话
-- =============================================================================

CREATE TABLE chat_sessions (
    session_id     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id        UUID        NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    title          TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_active_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE chat_sessions IS '对话会话';

-- 对话消息：按 created_at 做月度 RANGE 分区
CREATE TABLE chat_messages (
    message_id  UUID        NOT NULL DEFAULT gen_random_uuid(),
    session_id  UUID        NOT NULL REFERENCES chat_sessions(session_id) ON DELETE CASCADE,
    sender_type VARCHAR(8)  NOT NULL,      -- user / ai / system
    content     TEXT        NOT NULL,
    metadata    JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- token 数、引用来源等
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),        -- 分区键
    CONSTRAINT pk_chat_messages PRIMARY KEY (message_id, created_at),
    CONSTRAINT ck_chat_messages_sender CHECK (sender_type IN ('user', 'ai', 'system'))
) PARTITION BY RANGE (created_at);
COMMENT ON TABLE chat_messages IS '对话消息（按月分区）';

CREATE TABLE IF NOT EXISTS chat_messages_default PARTITION OF chat_messages DEFAULT;

-- 预建 chat_messages 当月 及 未来 2 个月 的分区
DO $$
DECLARE d date;
BEGIN
    FOR d IN
        SELECT generate_series(
                   date_trunc('month', now())::date,
                   (date_trunc('month', now()) + interval '2 month')::date,
                   interval '1 month'
               )::date
    LOOP
        PERFORM ensure_monthly_partition('chat_messages', d);
    END LOOP;
END $$;


-- =============================================================================
-- 7. 向量存储（pgvector 作为库内向量表；亦可外置 Milvus/Qdrant）
-- =============================================================================

CREATE TABLE embeddings (
    embedding_id UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_type   VARCHAR(24) NOT NULL,     -- raw_data / report_section / chat_message
    owner_id     UUID        NOT NULL,     -- 指向来源记录
    chunk_index  INT         NOT NULL DEFAULT 0,
    chunk_text   TEXT        NOT NULL,
    embedding    vector(1024) NOT NULL,   -- 维度必须与 EMBEDDING_MODEL 一致（示例用 BAAI/bge-m3 = 1024）
    model_name   VARCHAR(64) NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_embeddings_owner CHECK (
        owner_type IN ('raw_data', 'report_section', 'chat_message')
    )
);
COMMENT ON TABLE embeddings IS '文本切块向量表（RAG 检索用）';


-- =============================================================================
-- 8. 索引策略
-- =============================================================================

-- 8.1 用户与权限
CREATE INDEX ix_user_roles_role_id ON user_roles (role_id);

-- 8.2 个性化配置
CREATE INDEX ix_user_module_prefs_module_id ON user_module_prefs (module_id);

-- 8.3 原始数据（父表建索引会自动传播到各分区）
CREATE INDEX ix_raw_data_publish_time ON raw_data (publish_time DESC);
CREATE INDEX ix_raw_data_source_id    ON raw_data (source_id);
CREATE INDEX ix_raw_data_module_id    ON raw_data (module_id);
CREATE INDEX ix_raw_data_content_hash ON raw_data (source_id, content_hash);  -- 去重查询
-- 全文检索：title + content 表达式 GIN 索引
CREATE INDEX ix_raw_data_fts ON raw_data
    USING GIN (to_tsvector('simple'::regconfig,
                           coalesce(title, '') || ' ' || coalesce(content, '')));
-- trigram 索引：中文/短词模糊匹配更友好
CREATE INDEX ix_raw_data_content_trgm ON raw_data USING GIN (content gin_trgm_ops);
-- JSONB GIN 索引
CREATE INDEX ix_raw_data_metadata_gin ON raw_data USING GIN (metadata jsonb_path_ops);

-- 8.4 报告
CREATE INDEX ix_reports_user_time       ON reports (user_id, generated_time DESC);
CREATE INDEX ix_report_sections_report  ON report_sections (report_id);
CREATE INDEX ix_report_sections_type ON report_sections (section_type);
CREATE INDEX ix_report_sections_fts ON report_sections
    USING GIN (to_tsvector('simple'::regconfig, content));
CREATE INDEX ix_report_sources_data ON report_sources (data_id, data_publish_time);

-- 8.5 对话
CREATE INDEX ix_chat_sessions_user_time ON chat_sessions (user_id, last_active_at DESC);
CREATE INDEX ix_chat_messages_session   ON chat_messages (session_id, created_at DESC);

-- 8.6 向量（HNSW 近似最近邻，余弦距离）
CREATE INDEX ix_embeddings_hnsw ON embeddings
    USING hnsw (embedding vector_cosine_ops);
CREATE INDEX ix_embeddings_owner ON embeddings (owner_type, owner_id);


-- =============================================================================
-- 9. updated_at 触发器挂载
-- =============================================================================
CREATE TRIGGER trg_users_updated
    BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER trg_data_sources_updated
    BEFORE UPDATE ON data_sources
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- =============================================================================
-- 10. 冷热数据生命周期管理（近 3 个月热数据，1 年以上归档）
--     通过“归档 schema + 分区 DETACH”实现冷热分离，无需 DELETE 大表。
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS archive;

-- 将超过 p_keep_months 个月的 raw_data 分区从主表摘除并移入 archive schema。
CREATE OR REPLACE FUNCTION archive_old_partitions(p_keep_months int DEFAULT 12)
RETURNS void AS $$
DECLARE
    r record;
    v_cutoff date := (date_trunc('month', now()) - (p_keep_months || ' month')::interval)::date;
BEGIN
    FOR r IN
        SELECT c.relname AS part_name
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid      -- 分区子表
        JOIN pg_class p ON p.oid = i.inhparent     -- 分区父表
        WHERE p.relname = 'raw_data'
    LOOP
        -- 从分区名解析出月份，如 raw_data_2024_01 → 2024-01-01
        BEGIN
            IF to_date(right(r.part_name, 7), 'YYYY_MM') < v_cutoff THEN
                EXECUTE format('ALTER TABLE raw_data DETACH PARTITION %I', r.part_name);
                EXECUTE format('ALTER TABLE %I SET SCHEMA archive', r.part_name);
            END IF;
        EXCEPTION WHEN others THEN
            -- 跳过 default 分区或非标准命名分区
            CONTINUE;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 说明：热数据查询视图（近 3 个月），可选的读取加速手段
CREATE OR REPLACE VIEW v_hot_raw_data AS
    SELECT * FROM raw_data
    WHERE publish_time >= (now() - interval '3 month');


-- =============================================================================
-- 11. 初始化字典数据
-- =============================================================================
INSERT INTO roles (role_name) VALUES ('Manager'), ('Sales'), ('Tech')
ON CONFLICT (role_name) DO NOTHING;

INSERT INTO modules (module_code, module_name) VALUES
    ('news',     '新闻'),
    ('politics', '政治'),
    ('business', '商业统计'),
    ('finance',  '金融')
ON CONFLICT (module_code) DO NOTHING;


-- =============================================================================
-- 12. 【备选方案】雪花算法 BIGINT 主键
-- -----------------------------------------------------------------------------
-- 若团队要求使用 BIGINT 雪花 ID（便于分库分表、有序递增），可：
--   1) 应用侧用 snowflake 生成 ID 后显式写入；
--   2) 或在库内用“时间戳 + 序列”近似实现（仅示意，正式建议应用侧生成）：
--
--   CREATE SEQUENCE seq_snowflake START 1;
--   CREATE OR REPLACE FUNCTION next_snowflake() RETURNS BIGINT AS $$
--   BEGIN
--       -- 41bit(毫秒时间戳) << 22 | 10bit(机器) << 12 | 12bit(序列)
--       RETURN (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT << 22
--              | (1 << 12)
--              | (nextval('seq_snowflake') & 4095);
--   END; $$ LANGUAGE plpgsql;
--
--   使用时把 users.user_id 等列类型由 UUID 改为 BIGINT DEFAULT next_snowflake() 即可。
-- =============================================================================
