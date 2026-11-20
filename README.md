# 归档日志段 · 压缩字典换版服务

面向日志采集平台值班员的归档服务：上传一个 zlib **预置字典**（preset
dictionary）并启用，之后入库的日志段都用新版字典压缩；旧段在后台分批重压。
换版全程不中断入库和下载，单段重压失败保留原段，进程重启后自动续转。

- 纯 Python 标准库实现（`http.server` + `sqlite3` + `zlib`），无第三方依赖
- 日志段本体存为 **raw DEFLATE** 流，字典只作为 `zdict` 参与压缩/解压
- 元数据在 SQLite（WAL 模式）中，字典文件按内容寻址单独保存
- 按段号下载时现场解压，返回的字节与上传时逐字节一致，并附带 SHA-256

## 快速开始

```bash
docker compose up --build -d
# 打开 http://localhost:8080
```

数据落在命名卷 `archive-data`（容器内 `/data`）。也可用环境变量覆盖端口：

```bash
PORT=9090 docker compose up --build -d
```

本地不起容器直接跑（Python ≥ 3.10；容器镜像用 3.12）：

```bash
DATA_DIR=./data PORT=8080 python3 -m logarchive.app
```

## 网页上能做什么

| 区域 | 操作 |
| --- | --- |
| 当前状态 | 生效字典、段数、压缩前后体积、待换版数量、后台工作状态、最近失败原因（可一键重试） |
| 压缩字典 | 上传 ≤ 32 KiB 的字典（可勾选“立即启用”）、启用某个版本、删除不再被引用的版本 |
| 上传日志段 | 多选或拖入文件，逐个上传并显示进度与结果，每个文件分配一个段号 |
| 归档段 | 分页列表：段号、文件名、原始/压缩大小、所用字典、换版状态；按段号下载原文 |

命令行客户端（便于脚本化核对）：

```bash
python3 -m logarchive.cli status
python3 -m logarchive.cli dict-upload access-v2.dict --name v2 --activate
python3 -m logarchive.cli upload seg-0001.log seg-0002.log
python3 -m logarchive.cli segments --limit 20
python3 -m logarchive.cli download 42 --out /tmp/restored.log
python3 -m logarchive.cli wait
```

## 语义保证

### 字典换版

- 启用新字典是一个事务：同一时刻只有一个字典处于生效状态（SQLite 部分唯一索引
  `dictionaries_one_active` 强制）。
- **换版前就开始上传的段**：入库时在写事务里再次核对生效字典。若上传期间字典
  已经切换，这段会丢弃刚压好的结果、用新字典重新压缩后再入库——因此“新字典启用
  后入库的段一律用新版压缩”对飞行中的上传同样成立。
- 段号由 SQLite `AUTOINCREMENT` 分配，单调递增且不复用。

### 旧段重压

- 换版时把所有“不是新字典”的段标记为 `pending`，后台工作线程按段号顺序分批
  （`RECOMPRESS_BATCH_SIZE`）处理，每段处理完休息 `RECOMPRESS_DELAY_MS` 毫秒，
  避免和前台请求抢 IO。
- 单段转换流程：按记录用 **旧字典** 解压 → 校验 SHA-256/长度 → 用 **新字典** 重压
  → 再解压校验 → 新的 blob 原子 rename 进 `blobs/` → 在一个事务里切换到新 blob 和
  新字典 → 删除旧 blob。旧 blob 只在新记录提交之后才删除。
- 转换期间段仍可下载：下载读取元数据后用**已打开的**文件描述符解压（POSIX 上即使
  文件随后被 unlink 也能读完），万一撞上 rename/unlink 会重新读元数据重试。
- 转换期间段仍可入库：压缩与校验都在数据库事务之外完成，只在最后短暂持写锁。

### 失败与中断

- 任何异常（解压失败、校验不一致、磁盘错误……）都只回滚**这一次转换**：原段、原字典、
  原 blob 保持不变，段仍可下载；记录错误并指数退避重试。
- 连续失败 `MAX_RECOMPRESS_ATTEMPTS` 次后标记为 `failed`，不再自动重试；页面显示
  原因并可单段或“全部重试”。`failed` 的段**不**计入待换版数量。
- 进程重启时：
  - 状态为 `running` 的段（说明上次死在转换中途）重新排队——旧 blob 从未被替换，
    所以重头再来是安全的；
  - `failed` 的段获得新的重试预算；
  - `blobs/`、`dictionaries/`、`tmp/` 中的孤儿文件被清理（仅在工作线程启用的进程里做，
    避免和数据目录共享的只读副本互相踩）。
- 重压在目标字典不再是生效字典时（又换版了）会放弃该次结果并重新排队，不会写入
  过期版本。

### 字典删除

- 生效中的字典不能删除（`409 dictionary_active`）。
- 仍有段引用（`segments.dictionary_id` 指向它）时不能删除
  （`409 dictionary_in_use`，附 `segment_count`）。
- 删除后，所有使用过它的段都已经换成别的字典，按段号下载仍返回**相同字节**——
  下载解压只依赖元数据里记录的“当前字典”，不依赖历史字典文件。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 网页 |
| GET | `/healthz` | 健康检查 |
| GET | `/api/status` | 总览：生效字典、段数、体积、待换版、工作状态、最近错误 |
| GET | `/api/dictionaries` | 字典列表（含引用段数） |
| POST | `/api/dictionaries` | `multipart/form-data`：`dictionary`（文件，≤32 KiB）、`name`（可选）、`activate=1`（可选） |
| POST | `/api/dictionaries/{id}/activate` | 启用 |
| DELETE | `/api/dictionaries/{id}` | 删除（生效中或仍被引用时 409） |
| GET | `/api/segments?limit=&offset=` | 分页列出段 |
| GET | `/api/segments/{n}` | 单段元数据 |
| POST | `/api/segments` | `multipart/form-data`：`file`；可选头 `X-Content-SHA256` 校验 |
| GET | `/api/segments/{n}/download` | 按段号下载原文，响应头带 `X-Segment-Sha256`；支持单区间 `Range` |
| POST | `/api/recompression/retry` | JSON `{"segment": n}` 重排单段；空 body 重排全部 `pending`/`failed` |

错误响应统一为 `{"error": {"code": "...", "message": "...", "details": {...}}}`。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/data` | 数据根目录 |
| `PORT` / `HOST` | `8080` / `0.0.0.0` | 监听地址（compose 映射 `${PORT}`） |
| `MAX_SEGMENT_BYTES` | `536870912` | 单段上限（512 MiB） |
| `VERIFY_WRITES` | `true` | 每次压缩后立即解压校验 |
| `RECOMPRESS_DELAY_MS` | `100` | 后台每段之间的间隔 |
| `RECOMPRESS_BATCH_SIZE` | `8` | 一次领取的段数 |
| `MAX_RECOMPRESS_ATTEMPTS` | `5` | 单段连续失败上限 |
| `WORKER_ENABLED` | `true` | 关闭后只入库/下载，不自动重压 |
| `LOG_LEVEL` | `INFO` | 日志级别 |

## 数据目录结构

```
/data
├── archive.sqlite3      # 段与字典元数据（WAL）
├── blobs/<uuid>.deflate # 段本体，raw DEFLATE
├── dictionaries/<id>.dict
└── tmp/                 # 上传/下载/转换的临时文件，启动时清空
```

## 已知限制

- 单进程设计：SQLite + 本地文件系统，不支持多副本同时写入（worker 只有一个）。
  若要多实例，需要把存储换成共享对象存储并加分布式锁。
- 重压是逐段串行的（单 worker），段特别多时排队时间较长；吞吐取决于磁盘解压/压缩速度。
- 段本体在下载时整段解压到临时目录再返回（支持单区间 Range，但不支持多区间）。
- 上传/下载都没有认证与配额，按“内网值班工具”假设部署；对外暴露前应加反向代理鉴权。
- 未做压缩字典的“自动训练”；字典内容仍由值班员用日志样本自行生成后上传。
- `VERIFY_WRITES=true` 时每段会多一次完整读写校验，吞吐会下降，生产可按需关闭。

## 测试

```bash
python3 -m unittest discover -s tests -v        # 37 个用例：服务层 + HTTP 层
python3 scripts/acceptance_walkthrough.py --url http://localhost:8080
python3 scripts/interruption_test.py            # 自带服务进程，SIGKILL 中断后续转
```

`scripts/acceptance_walkthrough.py` 会真实地在上传进行到一半时激活新字典，然后
核对每个段按段号下载的字节是否与原文件完全一致、字典删除规则是否符合预期；
加 `--restart-cmd "<command>"` 或 `--docker` 还会在待换版队列非空时重启服务并
验证续转。
