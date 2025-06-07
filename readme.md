# Python 自动化脚本项目

## 项目简介

本项目旨在通过 Python 实现多种自动化数据处理、同步、分析与运维任务。项目结构清晰，支持模块化开发，便于跨平台（Windows 开发，Linux 服务器运行）维护和扩展。适用于数据分析、ETL、数据库同步、API 数据采集等多种业务场景。

---

## 目录结构说明

```
/python/auto_scripts/
├── .cursor/         # 项目规则与开发文档（勿随意更改）
├── .vscode/         # VSCode 配置
├── cache/           # 缓存文件
├── config/          # 敏感配置文件（如API、数据库等，严禁外泄）
├── history_version/ # 历史版本归档
├── log/             # 日志文件
├── jobs/            # 主要自动化任务脚本（不可复用，含业务主逻辑）
├── modules/         # 可复用的Python模块（如数据库、日志、邮件等工具）
├── scripts/         # 入口脚本（调度 jobs/modules 完成定时任务）
├── sql/             # SQL 脚本与查询配置
├── .env             # 环境变量
├── environment.yaml # Python依赖环境
└── readme.md        # 项目说明文档
```

---

## 主要功能模块

### 1. 自动化任务（`/jobs`）

- **wework/** 企业微信相关自动化脚本
  - `get_wework_token.py` 获取企业微信 token
- **moxueyuan/** 魔学院相关自动化脚本
  - `get_mxy_employee.py` 获取员工信息
  - `get_mxy_token.py` 获取魔学院 token
- **optimize/** 数据库优化与分析
  - `analyze_mydb.py` 数据库分析
  - `optimize_mydb.py` 优化脚本
  - `vacuum_mydwh.py` 数据仓库清理
- **sync/** 数据同步任务
  - `daily_database_sync.py` 日常数据库同步
  - `daily_dwh_sync.py` 日常数据仓库同步

### 2. 可复用模块（`/modules`）

- `access_token.py` 统一 token 获取与管理
- `db_conn.py` 数据库连接工具
- `directory.py` 目录操作工具
- `email_sender.py` 邮件发送工具
- `log_tools.py` 日志工具
- `token_managers/` token 管理子模块

### 3. 入口脚本（`/scripts`）

- `test.py` 示例或测试入口
- `optimize/`、`sync/` 目录下可扩展更多入口脚本

### 4. SQL 脚本与配置（`/sql`）

- **config/** 日常同步与查询配置（如 `daily_database_query.yaml`）
- **metabase/** 业务分析 SQL 脚本（如 `新客留存率.sql`、`经营岗业绩数据.sql` 等）
- **refresh_zcw/** 数据仓库刷新相关 SQL（如 `order_fact_refresh.sql`）

### 5. 配置文件（`/config`）

- `api.yaml`、`api_post_body.yaml` 等，存放敏感 API 配置

---

## 使用说明

1. **环境准备**
   - 安装 Python 3.8+
   - 安装依赖：`pip install -r environment.yaml` 或使用 conda 环境
   - 配置 `.env`、`/config` 下敏感信息（如数据库、API）

2. **运行入口**
   - 通过 `/scripts` 下入口脚本调度自动化任务
   - 具体任务逻辑在 `/jobs` 下实现，通用工具在 `/modules` 下

3. **日志与调试**
   - 日志文件输出至 `/log` 目录
   - 建议定期清理 `/cache`、`/log`、`/history_version` 目录

---

## 规范与约定

- **目录/文件命名**：全部采用小写+下划线（snake_case）
- **时间统一**：所有调度、日志、邮件等时间均采用 `Asia/Shanghai` (UTC+8)
- **敏感信息**：`/config` 目录严禁外泄，勿上传至代码仓库
- **代码复用**：通用工具请写在 `/modules`，业务主逻辑写在 `/jobs`
- **SQL 管理**：所有 SQL 脚本与配置集中在 `/sql` 目录

---