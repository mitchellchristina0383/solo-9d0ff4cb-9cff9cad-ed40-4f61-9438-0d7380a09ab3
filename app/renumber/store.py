"""换号工单持久化层：SQLite（WAL）+ 进程内互斥的事务化状态机。

网段生命周期
------------
- free      池内空闲，可被新工单领取
- in_use    某条链路台账上的现用 /31（登记时建立）
- reserved  已被未完工单领取，完工前既不能再分配，也不能交回池外

关键不变式（全部在写事务内维护）
--------------------------------
1. 同一 /31 任一时刻只处于 free / in_use / reserved 之一。
2. 一条链路至多持有一个 open 工单；工单号全局唯一，重提返回原分配。
3. 只有两端回执都确认，工单才 completed：旧址所在网段转 free、新网段
   转 in_use 并更新链路台账；单端回执不释放任何网段。
"""

from __future__ import annotations

import ipaddress
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from .models import (
    EndpointProgress,
    LinkView,
    PoolResponse,
    ReceiptRequest,
    ReceiptResponse,
    RegistryRequest,
    RegistryResponse,
    SubnetView,
    WorkOrderRequest,
    WorkOrderResponse,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    pool TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS subnets (
    seq INTEGER PRIMARY KEY,
    network TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('free', 'in_use', 'reserved')),
    link_id TEXT,
    work_order_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_subnets_status ON subnets(status);
CREATE TABLE IF NOT EXISTS links (
    link_id TEXT PRIMARY KEY,
    ep_a_name TEXT NOT NULL,
    ep_b_name TEXT NOT NULL,
    addr_a TEXT NOT NULL,
    addr_b TEXT NOT NULL,
    current_subnet TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS work_orders (
    work_order_id TEXT PRIMARY KEY,
    link_id TEXT NOT NULL REFERENCES links(link_id),
    status TEXT NOT NULL CHECK (status IN ('open', 'completed')),
    old_subnet TEXT NOT NULL,
    new_subnet TEXT NOT NULL,
    old_addr_a TEXT NOT NULL,
    old_addr_b TEXT NOT NULL,
    new_addr_a TEXT NOT NULL,
    new_addr_b TEXT NOT NULL,
    ep_a_name TEXT NOT NULL,
    ep_b_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_wo_link_status ON work_orders(link_id, status);
CREATE TABLE IF NOT EXISTS receipts (
    work_order_id TEXT NOT NULL REFERENCES work_orders(work_order_id),
    endpoint TEXT NOT NULL CHECK (endpoint IN ('a', 'b')),
    removed_addr TEXT NOT NULL,
    activated_addr TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (work_order_id, endpoint)
);
"""


class RenumberError(Exception):
    """业务错误基类，携带 HTTP 状态码。"""

    status_code = 409


class NotInitializedError(RenumberError):
    status_code = 409


class AlreadyInitializedError(RenumberError):
    status_code = 409


class PoolExhaustedError(RenumberError):
    status_code = 409


class ConflictError(RenumberError):
    status_code = 409


class NotFoundError(RenumberError):
    status_code = 404


class ValidationFailure(RenumberError):
    status_code = 422


class StoreWriteError(RenumberError):
    status_code = 500


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def split_slash31(pool_cidr: str) -> list[ipaddress.IPv4Network]:
    """把地址池按顺序切成 /31 网段列表（seq 即列表下标）。"""
    pool = ipaddress.ip_network(pool_cidr, strict=True)
    return list(pool.subnets(new_prefix=31))


def slash31_of(addr: str) -> ipaddress.IPv4Network:
    return ipaddress.ip_network(f"{addr}/31", strict=False)


class RenumberStore:
    """单库单实例状态机；线程安全，进程重启后从同一文件恢复。"""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        # 故障注入点（仅供测试）：置为 Exception 子类后，首个写事务在提交前抛出，
        # 事务回滚，验证“写入失败保持原状态”。
        self.inject_write_error: type[Exception] | None = None
        directory = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(directory, exist_ok=True)
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.executescript(_SCHEMA)
            conn.commit()
        finally:
            conn.close()

    # ------------------------------------------------------------ 事务工具
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """写事务：BEGIN IMMEDIATE 立即取库级写锁，配合进程锁串行化。"""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                if self.inject_write_error is not None:
                    raise self.inject_write_error("injected write failure before commit")
                conn.commit()
            except RenumberError:
                conn.rollback()
                raise
            except sqlite3.Error as exc:
                conn.rollback()
                raise StoreWriteError(f"数据库写入失败，已回滚保持原状态：{exc}") from exc
            except Exception as exc:
                # 含测试故障注入：任何提交前意外都回滚，不留下半成品状态
                conn.rollback()
                raise StoreWriteError(f"写入失败，已回滚保持原状态：{exc}") from exc
            finally:
                conn.close()

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            conn = self._connect()
            try:
                yield conn
            finally:
                conn.close()

    def reset(self) -> None:
        """仅供测试：清空全部换号数据。"""
        with self._tx() as conn:
            conn.executescript(
                "DELETE FROM receipts; DELETE FROM work_orders; "
                "DELETE FROM links; DELETE FROM subnets; DELETE FROM meta;"
            )

    def _require_meta(self, conn: sqlite3.Connection) -> sqlite3.Row:
        row = conn.execute("SELECT pool FROM meta WHERE id = 1").fetchone()
        if row is None:
            raise NotInitializedError("尚未登记地址池与链路台账，请先调用 POST /renumber/registry")
        return row

    # ------------------------------------------------------------ 预登记
    def register(self, req: RegistryRequest) -> RegistryResponse:
        pool_net = ipaddress.ip_network(req.pool, strict=True)
        all31 = split_slash31(req.pool)

        # ---- 跨记录校验（事务外先做纯计算，失败不触碰库）
        seen_links: set[str] = set()
        seen_subnets: dict[str, str] = {}
        seen_addrs: dict[str, str] = {}
        norm_links = []
        for lk in req.links:
            if lk.link_id in seen_links:
                raise ValidationFailure(f"链路标识重复：{lk.link_id}")
            seen_links.add(lk.link_id)
            if lk.addr_a == lk.addr_b:
                raise ValidationFailure(f"链路 {lk.link_id} 两端地址不能相同")
            net_a = slash31_of(lk.addr_a)
            net_b = slash31_of(lk.addr_b)
            if net_a != net_b:
                raise ValidationFailure(
                    f"链路 {lk.link_id} 的两端地址 {lk.addr_a}/{lk.addr_b} 不属于同一个 /31"
                )
            if not net_a.subnet_of(pool_net):
                raise ValidationFailure(f"链路 {lk.link_id} 的 /31 {net_a} 不在地址池 {req.pool} 内")
            if str(net_a) in seen_subnets:
                raise ValidationFailure(
                    f"网段 {net_a} 同时登记给链路 {seen_subnets[str(net_a)]} 与 {lk.link_id}"
                )
            for addr in (lk.addr_a, lk.addr_b):
                if addr in seen_addrs:
                    raise ValidationFailure(
                        f"地址 {addr} 同时登记给链路 {seen_addrs[addr]} 与 {lk.link_id}"
                    )
            seen_subnets[str(net_a)] = lk.link_id
            seen_addrs[lk.addr_a] = lk.link_id
            seen_addrs[lk.addr_b] = lk.link_id
            norm_links.append((lk, net_a))

        with self._tx() as conn:
            if conn.execute("SELECT 1 FROM meta WHERE id = 1").fetchone() is not None:
                raise AlreadyInitializedError("地址池与台账已登记，换号系统不支持重复登记或扩容")

            ts = _now()
            conn.execute("INSERT INTO meta(id, pool, created_at) VALUES (1, ?, ?)", (req.pool, ts))
            for seq, net in enumerate(all31):
                conn.execute(
                    "INSERT INTO subnets(seq, network, status) VALUES (?, ?, 'free')",
                    (seq, str(net)),
                )
            for lk, net in norm_links:
                conn.execute(
                    "UPDATE subnets SET status='in_use', link_id=? WHERE network=?",
                    (lk.link_id, str(net)),
                )
                conn.execute(
                    "INSERT INTO links(link_id, ep_a_name, ep_b_name, addr_a, addr_b, current_subnet)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (lk.link_id, lk.endpoint_a, lk.endpoint_b, lk.addr_a, lk.addr_b, str(net)),
                )
        return self.get_registry()

    # ------------------------------------------------------------ 开单 / 重提
    def create_work_order(self, req: WorkOrderRequest) -> tuple[WorkOrderResponse, bool]:
        """返回（工单视图, 是否首次创建）。重提原单返回原分配，replayed=True。"""
        with self._tx() as conn:
            self._require_meta(conn)
            existing = conn.execute(
                "SELECT * FROM work_orders WHERE work_order_id = ?", (req.work_order_id,)
            ).fetchone()
            if existing is not None:
                if existing["link_id"] != req.link_id:
                    raise ConflictError(
                        f"工单号 {req.work_order_id} 已用于链路 {existing['link_id']}，"
                        f"不能再开给链路 {req.link_id}"
                    )
                view = self._work_order_view(conn, existing)
                view.replayed = True
                return view, False

            link = conn.execute(
                "SELECT * FROM links WHERE link_id = ?", (req.link_id,)
            ).fetchone()
            if link is None:
                raise NotFoundError(f"链路 {req.link_id} 未登记")

            open_wo = conn.execute(
                "SELECT work_order_id FROM work_orders WHERE link_id = ? AND status = 'open'",
                (req.link_id,),
            ).fetchone()
            if open_wo is not None:
                raise ConflictError(
                    f"链路 {req.link_id} 已有未完工单 {open_wo['work_order_id']}，拒绝重复开单"
                )

            free_row = conn.execute(
                "SELECT seq, network FROM subnets WHERE status = 'free' ORDER BY seq LIMIT 1"
            ).fetchone()
            if free_row is None:
                raise PoolExhaustedError("地址池内已无空闲 /31 网段，无法开新单")

            new_net = ipaddress.ip_network(free_row["network"])
            new_addrs = sorted(new_net.hosts())  # /31：低位给 A 端，高位给 B 端
            new_a, new_b = str(new_addrs[0]), str(new_addrs[1])
            ts = _now()
            conn.execute(
                "UPDATE subnets SET status='reserved', work_order_id=? WHERE seq=?",
                (req.work_order_id, free_row["seq"]),
            )
            conn.execute(
                "INSERT INTO work_orders(work_order_id, link_id, status, old_subnet, new_subnet,"
                " old_addr_a, old_addr_b, new_addr_a, new_addr_b, ep_a_name, ep_b_name,"
                " created_at, completed_at) VALUES (?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                (
                    req.work_order_id,
                    req.link_id,
                    link["current_subnet"],
                    free_row["network"],
                    link["addr_a"],
                    link["addr_b"],
                    new_a,
                    new_b,
                    link["ep_a_name"],
                    link["ep_b_name"],
                    ts,
                ),
            )
            row = conn.execute(
                "SELECT * FROM work_orders WHERE work_order_id = ?", (req.work_order_id,)
            ).fetchone()
            return self._work_order_view(conn, row), True

    # ------------------------------------------------------------ 端点回执
    def submit_receipt(
        self, work_order_id: str, req: ReceiptRequest
    ) -> tuple[ReceiptResponse, bool]:
        """返回（回执后工单视图, 是否重复/迟到件）。地址对不上一律 422 且不记确认。"""
        with self._tx() as conn:
            self._require_meta(conn)
            wo = conn.execute(
                "SELECT * FROM work_orders WHERE work_order_id = ?", (work_order_id,)
            ).fetchone()
            if wo is None:
                raise NotFoundError(f"工单 {work_order_id} 不存在")

            expected_old = wo["old_addr_a"] if req.endpoint == "a" else wo["old_addr_b"]
            expected_new = wo["new_addr_a"] if req.endpoint == "a" else wo["new_addr_b"]
            if req.removed_addr != expected_old or req.activated_addr != expected_new:
                raise ValidationFailure(
                    f"工单 {work_order_id} {req.endpoint.upper()} 端回执地址对不上："
                    f"期望撤下 {expected_old}、启用 {expected_new}，"
                    f"实际撤下 {req.removed_addr}、启用 {req.activated_addr}"
                )

            prior = conn.execute(
                "SELECT 1 FROM receipts WHERE work_order_id = ? AND endpoint = ?",
                (work_order_id, req.endpoint),
            ).fetchone()
            already_confirmed = prior is not None

            if prior is None:
                conn.execute(
                    "INSERT INTO receipts(work_order_id, endpoint, removed_addr, activated_addr,"
                    " received_at) VALUES (?, ?, ?, ?, ?)",
                    (
                        work_order_id,
                        req.endpoint,
                        req.removed_addr,
                        req.activated_addr,
                        _now(),
                    ),
                )

            if wo["status"] == "open":
                confirmed = {
                    r["endpoint"]
                    for r in conn.execute(
                        "SELECT endpoint FROM receipts WHERE work_order_id = ?", (work_order_id,)
                    ).fetchall()
                }
                if {"a", "b"} <= confirmed:
                    # 两端齐备：新网段转在用、旧网段释放、台账换址，同一事务原子完成
                    conn.execute(
                        "UPDATE subnets SET status='in_use', link_id=?, work_order_id=NULL"
                        " WHERE network=?",
                        (wo["link_id"], wo["new_subnet"]),
                    )
                    conn.execute(
                        "UPDATE subnets SET status='free', link_id=NULL, work_order_id=NULL"
                        " WHERE network=?",
                        (wo["old_subnet"],),
                    )
                    conn.execute(
                        "UPDATE links SET addr_a=?, addr_b=?, current_subnet=? WHERE link_id=?",
                        (wo["new_addr_a"], wo["new_addr_b"], wo["new_subnet"], wo["link_id"]),
                    )
                    conn.execute(
                        "UPDATE work_orders SET status='completed', completed_at=?"
                        " WHERE work_order_id=?",
                        (_now(), work_order_id),
                    )

            row = conn.execute(
                "SELECT * FROM work_orders WHERE work_order_id = ?", (work_order_id,)
            ).fetchone()
            view = self._work_order_view(conn, row)
            return (
                ReceiptResponse(already_confirmed=already_confirmed, **view.model_dump()),
                already_confirmed,
            )

    # ------------------------------------------------------------ 查询
    def get_work_order(self, work_order_id: str) -> WorkOrderResponse:
        with self._read() as conn:
            self._require_meta(conn)
            row = conn.execute(
                "SELECT * FROM work_orders WHERE work_order_id = ?", (work_order_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"工单 {work_order_id} 不存在")
            return self._work_order_view(conn, row)

    def get_registry(self) -> RegistryResponse:
        with self._read() as conn:
            meta = conn.execute("SELECT pool FROM meta WHERE id = 1").fetchone()
            if meta is None:
                return RegistryResponse(initialized=False)
            links = [
                LinkView(
                    link_id=r["link_id"],
                    endpoint_a=r["ep_a_name"],
                    endpoint_b=r["ep_b_name"],
                    current_subnet=r["current_subnet"],
                    addr_a=r["addr_a"],
                    addr_b=r["addr_b"],
                )
                for r in conn.execute("SELECT * FROM links ORDER BY link_id").fetchall()
            ]
            total = conn.execute("SELECT COUNT(*) AS c FROM subnets").fetchone()["c"]
            return RegistryResponse(
                initialized=True,
                pool=meta["pool"],
                total_subnets=total,
                links=links,
            )

    def get_pool(self) -> PoolResponse:
        with self._read() as conn:
            self._require_meta(conn)
            rows = conn.execute("SELECT * FROM subnets ORDER BY seq").fetchall()
            views = [
                SubnetView(
                    seq=r["seq"],
                    subnet=r["network"],
                    status=r["status"],
                    link_id=r["link_id"],
                    work_order_id=r["work_order_id"],
                )
                for r in rows
            ]
            counts = {"free": 0, "in_use": 0, "reserved": 0}
            for r in rows:
                counts[r["status"]] += 1
            pool = conn.execute("SELECT pool FROM meta WHERE id = 1").fetchone()["pool"]
            return PoolResponse(
                pool=pool,
                total_subnets=len(rows),
                free=counts["free"],
                in_use=counts["in_use"],
                reserved=counts["reserved"],
                subnets=views,
            )

    # ------------------------------------------------------------ 视图组装
    def _work_order_view(
        self, conn: sqlite3.Connection, row: sqlite3.Row
    ) -> WorkOrderResponse:
        confirmed = {
            r["endpoint"]
            for r in conn.execute(
                "SELECT endpoint FROM receipts WHERE work_order_id = ?", (row["work_order_id"],)
            ).fetchall()
        }
        return WorkOrderResponse(
            work_order_id=row["work_order_id"],
            link_id=row["link_id"],
            status=row["status"],
            old_subnet=row["old_subnet"],
            new_subnet=row["new_subnet"],
            old_addr_a=row["old_addr_a"],
            old_addr_b=row["old_addr_b"],
            new_addr_a=row["new_addr_a"],
            new_addr_b=row["new_addr_b"],
            endpoint_a=EndpointProgress(name=row["ep_a_name"], confirmed="a" in confirmed),
            endpoint_b=EndpointProgress(name=row["ep_b_name"], confirmed="b" in confirmed),
            replayed=False,
            created_at=row["created_at"],
            completed_at=row["completed_at"],
        )
