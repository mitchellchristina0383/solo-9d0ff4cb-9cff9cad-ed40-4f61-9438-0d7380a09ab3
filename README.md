# 链路代价分批调整离线预检 + IPv4 /31 换号工单

本服务包含两个相互独立的子系统：

1. **链路代价分批调整离线环路预检**：提交连通路由拓扑、每条双向
   链路的新旧两版正整数代价、目标路由器和覆盖全部路由器的分批计划，服务按
   两版代价分别计算到目标的最短路径下一跳（等价路径的所有下一跳都可能被选
   中），逐批判断错峰切换过程中是否可能出现转发环路。
2. **IPv4 点对点链路 /31 换号工单**：预登记地址池与各链路两端现用地址，
   网管凭工单号为一条链路领取空闲 /31 及两端新地址；两端分别回报撤下的旧址
   与启用的新址，双端确认后才更新链路台账并释放旧网段。

## 判环模型

对每一批 `k`，切换期间各路由器的可用出边为：

| 路由器所属 | 切换期间可用出边 |
| --- | --- |
| 早于第 `k` 批 | 仅新版下一跳（前一批已全部生效） |
| 晚于第 `k` 批 | 仅旧版下一跳 |
| 第 `k` 批（本批） | **旧版 ∪ 新版**下一跳（设备可按任意顺序切换） |

在该并集转发图中：

- **存在有向环** ⇒ 存在一种错峰切换顺序与下一跳选择使流量兜圈，本批危险；
  环上仅新版可达的出边对应的本批设备，即为构造该环所需的“已切换设备”。
- **不存在有向环** ⇒ 本批切换的任意中间状态（任意已切换子集、任意等价
  下一跳选择）都不可能成环。

该等价关系由随机小拓扑暴力枚举（枚举全部切换子集 × 全部下一跳组合）
回归测试守护，见 `tests/test_api.py::test_union_lemma_against_bruteforce`。

下一跳由 Dijkstra 最短距离推导：无向边 `(u, v, w)` 上，`v` 是 `u` 的下一跳
当且仅当 `dist(u) == w + dist(v)`，等号成立的邻居全部纳入等价下一跳集合。

## 目录结构

```
app/
  main.py      FastAPI 入口（/、/health、/precheck、/docs，挂载 /renumber 路由）
  core.py      Dijkstra、下一跳集合、并集图与三色 DFS 找环
  models.py    预检请求/响应模型与输入校验
  renumber/    IPv4 换号工单子系统
    models.py  登记 / 工单 / 回执的 Pydantic 模型与 IPv4 字面量校验
    store.py   SQLite(WAL) 持久化状态机、/31 切分、事务化分配与双端确认
    api.py     /renumber/* HTTP 路由
tests/         pytest 自测（预检算法 + 换号工单全状态机/并发/回滚/重启）
examples/      可直接提交的示例请求（含 renumber_*.json 换号四步）
Dockerfile
docker-compose.yml
requirements.txt
```

## 启动（Docker Compose）

```bash
docker compose up --build
```

- 交互式文档：<http://localhost:8000/docs> （在 `/docs` 的 `POST /precheck`
  与 `/renumber/*` 上 "Try it out" 提交请求）
- 健康检查：`GET http://localhost:8000/health`
- 换号工单数据库保存在命名卷 `renumber-data`（容器内 `/data/renumber.db`），
  `docker compose down` 后重启数据仍在；`docker compose down -v` 才会清空。

无 Docker 时本地运行：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 请求格式

```json
{
  "target": "T",
  "links": [
    {"a": "T", "b": "A", "old_cost": 10, "new_cost": 30},
    {"a": "T", "b": "B", "old_cost": 20, "new_cost": 20},
    {"a": "A", "b": "B", "old_cost": 1, "new_cost": 1}
  ],
  "batches": [["A", "B", "T"]]
}
```

约束（违反返回 HTTP 422）：

- 代价为正整数；不允许自环、重复链路；拓扑必须连通。
- 分批计划覆盖所有路由器且不重不漏；单批最多 **50** 台；不允许空批次。
- 批次顺序即切换顺序，前一批全部生效后才进入下一批。

## 响应

安全：

```json
{"status": "pass", "target": "T", "batches_total": 3, "batches_checked": 3,
 "old_next_hops": {...}, "new_next_hops": {...}, "danger": null}
```

危险（首个危险批次）：

```json
{
  "status": "danger",
  "batches_checked": 1,
  "danger": {
    "batch_index": 1,
    "switched_devices": ["A"],
    "cycle_routers": ["A", "B", "A"],
    "forwarding_cycle": [
      {"router": "A", "next_hop": "B", "via": "new"},
      {"router": "B", "next_hop": "A", "via": "old"}
    ]
  }
}
```

`via` 标出该跳取自哪一版下一跳：`old` / `new` / `both`。

---

# IPv4 /31 换号工单

## 业务语义

点对点链路两端分头换号，**只撤掉一端旧地址时旧网段不能分给别的链路**。
网段在池内有三种状态，全部转移都在 SQLite 写事务内原子完成：

| 状态 | 含义 | 转移 |
| --- | --- | --- |
| `free` | 池内空闲 | 开单领取 → `reserved`；工单完工释放旧址 → `free` |
| `in_use` | 某条链路台账上的现用 /31 | 该链路工单双端确认后旧网段转 `free` |
| `reserved` | 已被未完工单领取 | 双端确认 → `in_use`（挂到链路台账） |

关键规则：

- 预登记一次性写入地址池（掩码 ≤ /31，自动按序切成 /31）与每条链路两端的
  现用地址；两端必须同属一个 /31，网段与地址均不允许跨链路重复。
- `POST /renumber/work-orders` 凭工单号开单：分配当前序号最小的空闲 /31，
  网段内低位地址给 A 端、高位给 B 端。
- **幂等重提**：同一工单号 + 同一链路重提，返回原分配（`replayed=true`，
  HTTP 200）；同一工单号打给别的链路返回 409。
- 链路已有未完工单时拒绝再开（409）；池内无空闲网段返回 409 且状态不变。
- 两端分别回报 `(endpoint, removed_addr, activated_addr)`，旧址、新址与工单
  完全一致才记为确认；对不上返回 422 且不落任何确认。
- **只有两端都确认**才在同一事务内：新网段 `reserved → in_use`、旧网段
  `in_use → free`、链路台账换址、工单置 `completed`。
- 重复 / 迟到回执（工单已完工后再发）按原工单、原端点幂等处理，
  `already_confirmed=true`，不重复记账、不改变状态。
- 并发分配经进程锁 + `BEGIN IMMEDIATE` 串行化，必定避开 `in_use` 与
  `reserved` 网段；写入失败整体回滚，保持原状态。
- 数据落 SQLite（WAL 模式）并在 compose 中挂载命名卷，重启后仍可查询
  分配结果与两端确认进度。

## 接口一览（均在 `/docs` 可试）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/renumber/registry` | 预登记地址池与链路现用地址（仅空库一次，201） |
| GET | `/renumber/registry` | 查询地址池与链路台账 |
| POST | `/renumber/work-orders` | 开单 / 重提（首次 201，重提 200） |
| GET | `/renumber/work-orders/{id}` | 查工单分配与两端确认进度 |
| POST | `/renumber/work-orders/{id}/receipts` | 单端回执 |
| GET | `/renumber/pool` | 池内全部 /31 的状态明细与计数 |

## 演示流程（示例文件见 `examples/`）

```bash
# 1. 预登记：10.0.0.0/28 切成 8 个 /31，前两个被 L1/L2 现用
curl -s localhost:8000/renumber/registry -H 'Content-Type: application/json' \
  -d @examples/renumber_registry.json
# 2. 为 L1 开单：领到首个空闲网段 10.0.0.4/31（A=.4, B=.5）
curl -s localhost:8000/renumber/work-orders -H 'Content-Type: application/json' \
  -d @examples/renumber_work_order.json
# 3. 原单重提：返回同一分配，replayed=true
curl -s localhost:8000/renumber/work-orders -H 'Content-Type: application/json' \
  -d @examples/renumber_work_order.json
# 4. 两端分别回执（只回一端时台账与旧网段保持不动）
curl -s localhost:8000/renumber/work-orders/WO-2026-0001/receipts \
  -H 'Content-Type: application/json' -d @examples/renumber_receipt_a.json
curl -s localhost:8000/renumber/work-orders/WO-2026-0001/receipts \
  -H 'Content-Type: application/json' -d @examples/renumber_receipt_b.json
# 5. 观察状态迁移：旧 /31 回池、新 /31 在用
curl -s localhost:8000/renumber/pool
curl -s localhost:8000/renumber/registry
```

数据文件路径由环境变量 `RENUMBER_DB_PATH` 控制，默认 `/data/renumber.db`。

## 自测

```bash
pip install -r requirements.txt pytest httpx
python -m pytest -q
```

覆盖：同批错峰成环、等价路径并集成环、安全多批次、第二批才成环、
单批 50 台上限等输入校验、断链拒绝、单路由器拓扑、结果确定性，
以及 40 组随机拓扑的并集判环 vs 全状态暴力枚举对照。

换号工单部分（`tests/test_renumber.py`）覆盖：登记校验（非 /31、越池、
网段/地址重复、重复登记）、开单与重提幂等、同号跨链路冲突、未完工单互斥、
未知资源 404、30 线程并发分配互斥且网段不撞、回执地址核对拒绝、单端回执
不释放旧网段、重复与迟到回执幂等、双端确认的台账切换与旧网段释放、
池耗尽、写入失败回滚、以及模拟重启后查询分配与两端进度并继续完工。

## 复杂度

每批构造并集图 `O(V+E)` 后做一次 `O(V+E)` 的确定性找环，共 `B` 批；
两次 Dijkstra 为 `O((V+E) log V)`。整体约 `O((V+E) log V + B(V+E))`。
