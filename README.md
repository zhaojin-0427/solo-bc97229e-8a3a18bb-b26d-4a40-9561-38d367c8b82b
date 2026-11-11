# 水库公示表格采集与入库系统（Hydro Table Collector）

把水库公示网页中的**合并单元格 HTML 表格**转换为可入库的**结构化观测记录**，
并提供从 URL 抓取、表格选择、表头指定、规则校验、预览、CSV 下载，到
**冻结批次 → 发送入库 → 幂等回执 → 失败重试**的完整 Web 流程。

系统由两个可独立重启的服务组成：

```
浏览器 ──▶ 采集端 Flask (8000) ──HTTP POST /api/batches──▶ 接收端 Flask (9001)
              │                                                │
              ▼                                                ▼
      collector.db (卷)                                receiver.db (卷)
      冻结批次/状态/回执                              整批原子入库/原回执
```

---

## 一、转换规则（采集端）

1. **采集网页（Flask）**：输入公示页 URL，抓取并列出页面内全部 HTML 表格，
   逐个给出容错迷你预览，选择目标表格并指定**顶部 1～3 行表头**。
2. **首列固定为水库名**：正文仅首列允许 `rowspan`，跨度内向下继承水库名；
   其余正文单元格不允许任何合并。
3. **按 `rowspan` / `colspan` 展开为规则网格**：
   - 表宽 = **首行展开宽度**（首行各单元格 colspan 之和）；
   - 同一来源单元格只记录一次，表头路径不重复同一段；
   - 路径文本相同的不同列仍分别保留，不合并。
4. **完整表头路径**：沿表头各行向下拼接，分隔符为 ` / `
   （如 `水位（米） / 旬初`）。
5. **空值语义**：观测空单元格保持为空；短行缺失的网格格位同样导出空值，
   且不携带来源单元格坐标。
6. **结构错误即停**：跨度重叠、横向越界、跨越表头/正文、正文非法合并时，
   标明来源单元格位置（`R行C列`，按**原表行序及行内单元格序从 1 计数**）
   并停止转换，不产生任何记录。
7. **CSV 导出**：字段为 `水库, 表头路径, 原始文本, 来源单元格行,
   来源单元格列, 来源单元格坐标, 来源URL`，UTF-8 带 BOM，Excel 可直接打开。

## 二、入库发送（新增）

### 冻结批次与幂等批次号

在转换预览页点击「**发送入库**」后，采集端把**本次转换的全部记录**序列化为
持久批次写入 `collector.db`，然后由后台发送到接收端：

- 批次号 `batch_id = "B-" + SHA-256(业务内容)`，业务内容包含来源 URL、
  表头路径和全部记录（不含时间戳）。
- 同一转换结果无论被几个标签页、在什么时间提交，都得到**相同批次号**；
  本地 SQLite 主键 + 接收端幂等逻辑共同保证**只建一批**。
- 批次一旦冻结，payload 原样保存；重试永远复用原批次号与原内容，
  不会重新读取当前页面或生成新批次。

### 状态机

| 状态 | 含义 | 页面行为 |
| --- | --- | --- |
| `unconfirmed` 未确认 | 连接中断、超时、5xx 或回执缺失，无法确认是否入库 | 显示失败原因，可重试 |
| `confirmed` 已确认 | 收到接收端回执 | 显示回执号、下游记录数 |
| `rejected` 已拒绝 | 下游明确拒绝（400/409，如同批次号内容冲突） | 显示原因，可重试 |

“下游已入库但回执丢失”正是 `unconfirmed` 的典型场景：接收端事务已提交，
连接在回执返回前断开；采集端保持未确认，重试时接收端发现同批次号同内容，
**返回首次入库的原回执**（`idempotent: true`），下游不会重复写入。

### 接收端入库语义

`POST /api/batches`：

- **整批原子入库**：批次主记录与全部观测记录在同一个
  `BEGIN IMMEDIATE` 事务中写入，任一步失败整体回滚，不会出现半批数据。
- **同批次号 + 同内容** → 200，返回**原回执**（不重复写入）；
- **同批次号 + 不同内容** → 409 `content_mismatch`，原批次保持不变；
- **载荷指纹被篡改/字段不合法** → 400 `invalid_payload`，不写入任何数据。

### 一次性“入库后断开连接”演示开关

接收端提供开关（接收端页面 `/` 或采集端 `入库发送记录` 页均可操作）：

1. 开启后，下一次**新批次**提交事务并落库；
2. 接收端在返回回执前强制关闭 TCP 连接（RST），采集端收到
   `Remote end closed connection without response`；
3. 开关自动关闭（一次性）；采集端批次保持 `unconfirmed`；
4. 点击重试，接收端返回首次入库的原回执，状态变为 `confirmed`，
   下游批次/记录数不变。

> 演示需要产生一个**新批次**。内置的「备用示例页」与标准示例结构相同、
> 数值不同，专用于此演示。

## 三、目录结构

```
.
├── app/
│   ├── web.py                  # 采集端路由：抓取/选择/转换/下载/发送/批次页
│   ├── grid_parser.py          # 合并单元格展开、校验、表头路径、记录生成
│   ├── contract.py             # 两端共享的批次载荷契约与内容指纹
│   ├── collector_store.py      # 采集端 SQLite：冻结、状态、回执、重启恢复
│   ├── sender.py               # 采集端 → 接收端 HTTP 发送/查询
│   ├── receiver.py             # 接收端 Flask：API、状态页、断连演示
│   ├── receiver_store.py       # 接收端 SQLite：整批原子入库、幂等回执
│   ├── templates/              # index/select/preview/deliveries/... 页面
│   └── static/
│       ├── style.css
│       └── samples/
│           ├── notice.html        # 标准示例（合法 rowspan/空值/短行）
│           ├── notice_alt.html    # 备用示例（数值不同，断连演示用）
│           └── notice_error.html  # 错误示例（结构错误定位）
├── tests/                      # 解析规则 + Web 全流程 + 入库/幂等/断连/持久化
├── requirements.txt
├── Dockerfile
├── docker-compose.yml          # 采集端 + 接收端 + 两个持久化卷
├── run_dev.sh                  # 本地启动采集端
└── run_receiver.sh             # 本地启动接收端
```

## 四、Docker Compose（推荐，交付即启）

```bash
docker compose up --build
```

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| 采集端 / 演示入口 | http://localhost:8000 | 抓取、转换、发送入库、状态与重试 |
| 接收端状态页 | http://localhost:9001 | 已收批次、记录数、断连开关 |
| 接收端 API | http://localhost:9001/api/batches | 供采集端调用 |

数据卷：

- `collector-data:/app/data` → `collector.db`（批次、状态、回执）
- `receiver-data:/app/data` → `receiver.db`（已收批次与记录）

`docker compose down` 后再次 `up`，两个卷仍保留；`down -v` 才会清空。

## 五、演示路径

### 1. 转换与正常入库

1. 打开 http://localhost:8000，点击「使用标准示例页」；
2. 选择表格、表头行数保持 **2 行**，点击「转换此表格」；
3. 预览 35 条观测记录（5 条正文行 × 7 个观测列）、完整表头路径、
   来源坐标；注意青山水库的 rowspan 继承、碧河空单元格、云岭短行空值；
4. 点击「**发送入库（冻结全部 35 条记录）**」；
5. 批次详情显示「已确认入库」、回执号与下游记录数；
6. 打开「入库发送记录」：本地 1 批，下游已收 1 批 / 35 条；
   也可打开 http://localhost:9001 查看接收端视角。

**多标签页只建一批**：对同一示例再转换一次（两个标签页/两次转换结果），
分别点击发送，会跳到**同一个批次号**，下游仍只有 1 批。

### 2. 回执丢失 → 重试幂等（一次性断连开关）

1. 在「入库发送记录」页开启「**入库后断开连接（一次性）**」；
2. 返回首页，点击「使用**备用示例页**」（与标准示例数值不同，会产生新批次）；
3. 转换后点击「发送入库」→ 详情页显示「**未确认**」，
   失败原因类似 `Connection reset by peer` / `Remote end closed connection`；
4. 此时打开接收端 http://localhost:9001：该批次**已经整批入库**
   （记录数 35），说明下游已落库、只是回执在断连中丢失；
5. 回到采集端批次详情点击「**重试发送（沿用原批次号与原内容）**」；
6. 状态变为「已确认入库」，回执 `idempotent: true`；
   下游批次/记录数不变，没有重复入库。

### 3. 重启后恢复

```bash
docker compose restart          # 或 docker compose down && docker compose up
```

- 采集端列表仍显示历史批次；若断连后**尚未重试**就重启，
  未确认批次仍在列表中并有完整冻结载荷，可直接继续重试，
  也可在页面点击「**恢复：重试全部未确认批次**」；
- 接收端已收批次与记录数保持不变。

### 4. 结构错误停止

首页点击「使用错误示例页」，分别转换两张表，观察带 `R行C列` 的报错与
“错误排除前不产生任何记录”的行为。

## 六、接口契约

`POST /api/batches` 请求体（由采集端 `app/contract.py` 生成）：

```json
{
  "batch_id": "B-<sha256>",
  "content_hash": "<sha256>",
  "source_url": "/sample/notice.html",
  "header_paths": ["...", "..."],
  "record_count": 35,
  "records": [
    {
      "reservoir": "青山水库",
      "header_path": "水位（米） / 旬初",
      "value": "128.40",
      "source_row": 3,
      "source_col": 2,
      "source_cell": "R3C2",
      "source_url": "/sample/notice.html"
    }
  ],
  "created_at": "2026-11-11T12:00:00+00:00"
}
```

接收端会重新计算 `content_hash` 并校验 `record_count`，不信任客户端声明。

成功回执：

```json
{
  "receipt_id": "RCPT-...",
  "batch_id": "B-...",
  "content_hash": "...",
  "record_count": 35,
  "received_at": "2026-11-11T12:00:01+00:00",
  "idempotent": true,
  "status": "accepted"
}
```

其他接口：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查 |
| GET | `/api/batches` | 下游已收批次列表、批次/记录总数 |
| GET | `/api/batches/<batch_id>` | 单批详情（含全部记录） |
| GET | `/api/demo/state` | 断连开关状态 |
| POST | `/api/demo/disconnect-next` | `{"enabled": true/false}` 设置一次性开关 |

## 七、本地 Python 运行

需要 Python 3.11+：

```bash
python3 -m pip install -r requirements.txt

# 终端 1：接收端（默认 9000）
RECEIVER_DB=$PWD/data/receiver.db ./run_receiver.sh

# 终端 2：采集端（默认 8000，指向本地接收端）
COLLECTOR_DB=$PWD/data/collector.db \
RECEIVER_URL=http://127.0.0.1:9000 \
RECEIVER_PUBLIC_URL=http://127.0.0.1:9000 \
./run_dev.sh
```

开发热更可用 `FLASK_APP=app.web flask run --port 8000`；
接收端也可直接 `python -m app.receiver`。

## 八、测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖：表宽确定、表头去重与同路径多列保留、rowspan 继承、空值/短行、
坐标 1 基计数、四类结构错误定位、错误即停、Flask 全流程与 CSV 内容；
以及批次指纹稳定性、载荷篡改拒绝、同批次同内容原回执、同批次不同内容
409、整批记录数、断连后未确认与重试恢复、采集端/接收端重启持久化。

## 九、设计约束与说明

- 只解析目标 `<table>` 的直属 `<tr>/<td>/<th>`，嵌套表格不会串入。
- `rowspan=0`、非法跨度属性按浏览器约定按 1 处理；垂直方向超出表格
  底部的 rowspan 按浏览器约定裁剪，**横向**超出表宽判越界报错。
- 采集端抓取缓存为进程内字典（TTL 30 分钟、上限 50 条），Compose 使用
  单 gunicorn worker + 多线程；持久批次与发送状态在 SQLite 中，重启不受影响。
- 接收端默认使用 Werkzeug 多线程服务器：只有该服务器会在 WSGI environ
  中暴露 `werkzeug.socket`，从而能在事务提交后真实关闭 TCP 连接来演示
  “回执丢失”。数据文件独立持久化，不依赖进程内存。
- 抓取远程页面时携带自定义 User-Agent、15 秒超时，并按响应头/内容
  推断编码，适配中文公示页常见的 GB 系编码。
