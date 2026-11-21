"""IPv4 /31 换号工单单子系统测试。

覆盖：预登记校验、开单/重提幂等、同号冲突、未完工单互斥、回执地址核对、
重复/迟到回执、双端确认的台账切换与旧网段释放、池耗尽、并发分配、
写入失败回滚、重启持久化。
"""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.renumber import api as renumber_api
from app.renumber.models import ReceiptRequest, WorkOrderRequest
from app.renumber.store import RenumberStore

client = TestClient(app)

POOL = "10.0.0.0/28"  # 8 个 /31：10.0.0.0/31 ... 10.0.0.14/31


@pytest.fixture()
def store(tmp_path, monkeypatch):
    """每个用例独立 SQLite 文件，并重置 API 层单例。"""
    db = tmp_path / "renumber.db"
    monkeypatch.setenv("RENUMBER_DB_PATH", str(db))
    renumber_api._store = None
    s = renumber_api.get_store()
    yield s
    renumber_api._store = None


def _registry_payload(pool: str = POOL) -> dict:
    return {
        "pool": pool,
        "links": [
            {
                "link_id": "L1",
                "endpoint_a": "R1",
                "endpoint_b": "R2",
                "addr_a": "10.0.0.0",
                "addr_b": "10.0.0.1",
            },
            {
                "link_id": "L2",
                "endpoint_a": "R3",
                "endpoint_b": "R4",
                "addr_a": "10.0.0.2",
                "addr_b": "10.0.0.3",
            },
        ],
    }


def _register(store, payload=None) -> dict:
    r = client.post("/renumber/registry", json=payload or _registry_payload())
    assert r.status_code == 201, r.text
    return r.json()


# ------------------------------------------------------------ 预登记校验
def test_registry_seeds_subnets_and_links(store):
    data = _register(store)
    assert data["initialized"] is True
    assert data["pool"] == POOL
    assert data["total_subnets"] == 8
    assert {lk["link_id"] for lk in data["links"]} == {"L1", "L2"}

    pool = client.get("/renumber/pool").json()
    assert pool["in_use"] == 2
    assert pool["reserved"] == 0
    assert pool["free"] == 6
    first = pool["subnets"][0]
    assert first["subnet"] == "10.0.0.0/31"
    assert first["status"] == "in_use"
    assert first["link_id"] == "L1"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.update(pool="10.0.0.0/32"),                 # 池比 /31 更小
        lambda p: p.update(pool="10.0.0.1/28"),                 # 主机位非零
        lambda p: p.update(pool="2001:db8::/64"),               # 非 IPv4
        lambda p: p["links"][0].update(addr_a="10.0.0.0", addr_b="10.0.0.3"),  # 两端不同 /31
        lambda p: p["links"][0].update(addr_a="9.9.9.9"),       # 旧址不在池内
        lambda p: p["links"][0].update(addr_a="10.0.0.2"),      # 与 L2 同网段
        lambda p: p["links"][1].update(addr_a="10.0.0.1"),      # 地址跨链路重复
    ],
)
def test_registry_validation_rejected(store, mutate):
    payload = _registry_payload()
    mutate(payload)
    r = client.post("/renumber/registry", json=payload)
    assert r.status_code == 422, r.text


def test_registry_rejects_duplicate_link_id(store):
    payload = _registry_payload()
    payload["links"][1]["link_id"] = "L1"
    r = client.post("/renumber/registry", json=payload)
    assert r.status_code == 422
    assert "重复" in r.json()["detail"]


def test_registry_only_once(store):
    _register(store)
    r = client.post("/renumber/registry", json=_registry_payload())
    assert r.status_code == 409
    # 拒绝后台账保持原样
    assert len(client.get("/renumber/registry").json()["links"]) == 2


def test_operations_before_registration_rejected(store):
    r = client.post(
        "/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "L1"}
    )
    assert r.status_code == 409


# ------------------------------------------------------------ 开单与重提
def test_create_work_order_allocates_first_free_slash31(store):
    _register(store)
    r = client.post("/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "L1"})
    assert r.status_code == 201, r.text
    data = r.json()
    assert data["status"] == "open"
    assert data["replayed"] is False
    # 前两个 /31 在用，首个空闲为 10.0.0.4/31；低位给 A、高位给 B
    assert data["new_subnet"] == "10.0.0.4/31"
    assert data["new_addr_a"] == "10.0.0.4"
    assert data["new_addr_b"] == "10.0.0.5"
    assert data["old_subnet"] == "10.0.0.0/31"
    assert data["old_addr_a"] == "10.0.0.0"
    assert data["endpoint_a"] == {"name": "R1", "confirmed": False}
    assert data["endpoint_b"]["confirmed"] is False

    pool = client.get("/renumber/pool").json()
    assert pool["reserved"] == 1
    assert pool["free"] == 5
    reserved = [s for s in pool["subnets"] if s["status"] == "reserved"][0]
    assert reserved["work_order_id"] == "WO1"
    # 旧网段仍 in_use：单端都未确认，不能释放
    assert reserved["link_id"] is None
    assert pool["subnets"][0]["status"] == "in_use"


def test_replay_same_work_order_returns_same_allocation(store):
    _register(store)
    first = client.post(
        "/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "L1"}
    ).json()
    r = client.post("/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "L1"})
    assert r.status_code == 200
    second = r.json()
    assert second["replayed"] is True
    for key in ("new_subnet", "new_addr_a", "new_addr_b", "old_subnet", "old_addr_a",
                "old_addr_b", "created_at"):
        assert second[key] == first[key]
    # 重提不重复占用网段
    assert client.get("/renumber/pool").json()["reserved"] == 1


def test_same_work_order_id_on_other_link_rejected(store):
    _register(store)
    client.post("/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "L1"})
    r = client.post("/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "L2"})
    assert r.status_code == 409
    assert "已用于" in r.json()["detail"]


def test_link_with_open_work_order_rejects_new_one(store):
    _register(store)
    client.post("/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "L1"})
    r = client.post("/renumber/work-orders", json={"work_order_id": "WO2", "link_id": "L1"})
    assert r.status_code == 409
    assert "未完工单" in r.json()["detail"]
    assert client.get("/renumber/pool").json()["reserved"] == 1


def test_unknown_link_and_work_order_404(store):
    _register(store)
    r = client.post("/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "NOPE"})
    assert r.status_code == 404
    r = client.get("/renumber/work-orders/WO1")
    assert r.status_code == 404


def test_concurrent_allocations_avoid_inuse_and_reserved(store):
    """/27 池内 16 个 /31，6 条链路在用、剩 10 个空闲。每链路 5 个并发开单：
    每链路恰好 1 个成功（共 6 个，网段两两不同且不碰现用网段），其余被互斥拒绝。"""
    payload = {
        "pool": "10.0.0.0/27",
        "links": [
            {
                "link_id": f"L{i}",
                "endpoint_a": f"R{2 * i}",
                "endpoint_b": f"R{2 * i + 1}",
                "addr_a": f"10.0.0.{2 * i}",
                "addr_b": f"10.0.0.{2 * i + 1}",
            }
            for i in range(6)
        ],
    }
    _register(store, payload)
    results: list = []
    barrier = threading.Barrier(30)

    def open_order(i: int) -> None:
        barrier.wait()
        link = f"L{i % 6}"
        r = client.post(
            "/renumber/work-orders",
            json={"work_order_id": f"WO{i}", "link_id": link},
        )
        results.append((i, r.status_code, r.json() if r.status_code == 201 else None))

    with ThreadPoolExecutor(max_workers=30) as pool_exec:
        list(pool_exec.map(open_order, range(30)))

    created = [r for r in results if r[1] == 201]
    rejected = [r for r in results if r[1] == 409]
    assert len(created) == 6
    assert len(rejected) == 24
    subnets = {r[2]["new_subnet"] for r in created}
    assert len(subnets) == 6
    # 现用 6 个 /31 绝不能被分走
    assert subnets.isdisjoint({f"10.0.0.{2 * i}/31" for i in range(6)})
    # 每条链路恰好一个未完工单
    assert {r[2]["link_id"] for r in created} == {f"L{i}" for i in range(6)}
    view = client.get("/renumber/pool").json()
    assert view["free"] == 4 and view["reserved"] == 6 and view["in_use"] == 6


# ------------------------------------------------------------ 回执与状态机
def _open_wo1(store) -> dict:
    _register(store)
    return client.post(
        "/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "L1"}
    ).json()


def test_single_receipt_keeps_ledger_and_old_subnet(store):
    wo = _open_wo1(store)
    r = client.post(
        "/renumber/work-orders/WO1/receipts",
        json={"endpoint": "a", "removed_addr": "10.0.0.0", "activated_addr": "10.0.0.4"},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["already_confirmed"] is False
    assert data["status"] == "open"
    assert data["endpoint_a"]["confirmed"] is True
    assert data["endpoint_b"]["confirmed"] is False
    # 只撤一端：台账不变、旧网段不释放、新网段继续 reserved
    link = [lk for lk in client.get("/renumber/registry").json()["links"] if lk["link_id"] == "L1"][0]
    assert link["addr_a"] == "10.0.0.0"
    assert link["current_subnet"] == "10.0.0.0/31"
    pool = client.get("/renumber/pool").json()
    assert pool["subnets"][0]["status"] == "in_use"
    assert pool["subnets"][2]["status"] == "reserved"


def test_receipt_wrong_address_rejected_without_confirm(store):
    _open_wo1(store)
    bad_cases = [
        {"endpoint": "a", "removed_addr": "10.0.0.1", "activated_addr": "10.0.0.4"},  # 旧址错
        {"endpoint": "a", "removed_addr": "10.0.0.0", "activated_addr": "10.0.0.5"},  # 新址错（拿反）
        {"endpoint": "b", "removed_addr": "10.0.0.0", "activated_addr": "10.0.0.5"},  # 端点错
    ]
    for body in bad_cases:
        r = client.post("/renumber/work-orders/WO1/receipts", json=body)
        assert r.status_code == 422, body
    view = client.get("/renumber/work-orders/WO1").json()
    assert view["endpoint_a"]["confirmed"] is False
    assert view["endpoint_b"]["confirmed"] is False


def test_duplicate_and_late_receipts_are_idempotent(store):
    _open_wo1(store)
    body_a = {"endpoint": "a", "removed_addr": "10.0.0.0", "activated_addr": "10.0.0.4"}
    body_b = {"endpoint": "b", "removed_addr": "10.0.0.1", "activated_addr": "10.0.0.5"}
    assert client.post("/renumber/work-orders/WO1/receipts", json=body_a).json()["already_confirmed"] is False
    dup = client.post("/renumber/work-orders/WO1/receipts", json=body_a).json()
    assert dup["already_confirmed"] is True
    assert dup["status"] == "open"
    # B 端确认，工单完工
    done = client.post("/renumber/work-orders/WO1/receipts", json=body_b).json()
    assert done["status"] == "completed"
    assert done["completed_at"] is not None
    # 迟到回执（完工后重发两端）：只按原工单原端点处理，不再改动状态
    late_a = client.post("/renumber/work-orders/WO1/receipts", json=body_a).json()
    late_b = client.post("/renumber/work-orders/WO1/receipts", json=body_b).json()
    assert late_a["already_confirmed"] is True
    assert late_b["already_confirmed"] is True
    assert late_a["status"] == "completed"


def test_both_confirmations_switch_ledger_and_release_old_subnet(store):
    _open_wo1(store)
    client.post(
        "/renumber/work-orders/WO1/receipts",
        json={"endpoint": "a", "removed_addr": "10.0.0.0", "activated_addr": "10.0.0.4"},
    )
    r = client.post(
        "/renumber/work-orders/WO1/receipts",
        json={"endpoint": "b", "removed_addr": "10.0.0.1", "activated_addr": "10.0.0.5"},
    )
    assert r.json()["status"] == "completed"

    pool = client.get("/renumber/pool").json()
    by_net = {s["subnet"]: s for s in pool["subnets"]}
    # 旧网段释放回池，新网段转在用并挂到 L1
    assert by_net["10.0.0.0/31"]["status"] == "free"
    assert by_net["10.0.0.0/31"]["link_id"] is None
    assert by_net["10.0.0.4/31"]["status"] == "in_use"
    assert by_net["10.0.0.4/31"]["link_id"] == "L1"
    assert pool["free"] == 6 and pool["in_use"] == 2 and pool["reserved"] == 0

    link = [lk for lk in client.get("/renumber/registry").json()["links"] if lk["link_id"] == "L1"][0]
    assert link["current_subnet"] == "10.0.0.4/31"
    assert link["addr_a"] == "10.0.0.4" and link["addr_b"] == "10.0.0.5"

    # 完工后同链路允许开下一单，且最先领取刚释放的旧网段（seq 最小的空闲）
    r = client.post("/renumber/work-orders", json={"work_order_id": "WO2", "link_id": "L1"})
    assert r.status_code == 201
    assert r.json()["new_subnet"] == "10.0.0.0/31"


def test_receipt_on_unknown_work_order_404(store):
    _register(store)
    r = client.post(
        "/renumber/work-orders/GHOST/receipts",
        json={"endpoint": "a", "removed_addr": "10.0.0.0", "activated_addr": "10.0.0.4"},
    )
    assert r.status_code == 404


# ------------------------------------------------------------ 池耗尽
def test_pool_exhausted_rejects_and_keeps_state(store):
    """小池：池内 2 个 /31 全部在用，开单无空闲。"""
    payload = {
        "pool": "10.0.0.0/30",
        "links": [
            {"link_id": "L1", "endpoint_a": "R1", "endpoint_b": "R2",
             "addr_a": "10.0.0.0", "addr_b": "10.0.0.1"},
            {"link_id": "L2", "endpoint_a": "R3", "endpoint_b": "R4",
             "addr_a": "10.0.0.2", "addr_b": "10.0.0.3"},
        ],
    }
    _register(store, payload)
    r = client.post("/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "L1"})
    assert r.status_code == 409
    assert "无空闲" in r.json()["detail"]
    pool = client.get("/renumber/pool").json()
    assert pool["free"] == 0 and pool["reserved"] == 0
    assert client.get("/renumber/work-orders/WO1").status_code == 404


# ------------------------------------------------------------ 写入失败回滚
def test_write_failure_rolls_back_and_state_unchanged(store, monkeypatch):
    _register(store)
    store.inject_write_error = sqlite3.IntegrityError
    with pytest.raises(Exception):
        store.create_work_order(WorkOrderRequest(work_order_id="WO1", link_id="L1"))
    store.inject_write_error = None

    # 网段与工单均未留下痕迹，可正常重新开单
    pool = client.get("/renumber/pool").json()
    assert pool["reserved"] == 0 and pool["free"] == 6
    r = client.post("/renumber/work-orders", json={"work_order_id": "WO1", "link_id": "L1"})
    assert r.status_code == 201
    assert r.json()["new_subnet"] == "10.0.0.4/31"


def test_receipt_write_failure_does_not_confirm(store):
    _open_wo1(store)
    store.inject_write_error = sqlite3.OperationalError
    with pytest.raises(Exception):
        store.submit_receipt(
            "WO1",
            ReceiptRequest(endpoint="a", removed_addr="10.0.0.0", activated_addr="10.0.0.4"),
        )
    store.inject_write_error = None

    view = client.get("/renumber/work-orders/WO1").json()
    assert view["endpoint_a"]["confirmed"] is False


# ------------------------------------------------------------ 重启持久化
def test_state_survives_store_restart(tmp_path, monkeypatch):
    from app.renumber.models import RegistryRequest

    db = tmp_path / "renumber.db"
    monkeypatch.setenv("RENUMBER_DB_PATH", str(db))
    renumber_api._store = None
    s1 = renumber_api.get_store()
    s1.register(RegistryRequest(**_registry_payload()))
    s1.create_work_order(WorkOrderRequest(work_order_id="WO1", link_id="L1"))
    s1.submit_receipt(
        "WO1", ReceiptRequest(endpoint="a", removed_addr="10.0.0.0", activated_addr="10.0.0.4")
    )

    # 模拟进程重启：丢弃单例，用同一数据文件重建
    renumber_api._store = None
    s2 = renumber_api.get_store()
    assert s2 is not s1
    view = s2.get_work_order("WO1")
    assert view.status == "open"
    assert view.endpoint_a.confirmed is True
    assert view.endpoint_b.confirmed is False
    assert view.new_subnet == "10.0.0.4/31"
    pool = s2.get_pool()
    assert pool.reserved == 1 and pool.in_use == 2 and pool.free == 5
    # 重启后补 B 端回执，仍可正常完工
    done, _ = s2.submit_receipt(
        "WO1", ReceiptRequest(endpoint="b", removed_addr="10.0.0.1", activated_addr="10.0.0.5")
    )
    assert done.status == "completed"
    assert s2.get_pool().free == 6
    renumber_api._store = None


def test_direct_store_concurrent_open_orders_serialize(tmp_path):
    """绕过 HTTP，直接压 store：同链路并发只能有一个 open 工单。"""
    db = tmp_path / "direct.db"
    s = RenumberStore(str(db))
    from app.renumber.models import RegistryRequest

    s.register(RegistryRequest(**_registry_payload()))
    errors: list[Exception] = []

    def open_order(i: int) -> None:
        try:
            s.create_work_order(WorkOrderRequest(work_order_id=f"D{i}", link_id="L1"))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=10) as ex:
        list(ex.map(open_order, range(10)))
    # 仅 1 个成功，其余 9 个都因未完工单互斥被拒
    assert len(errors) == 9
    assert s.get_pool().reserved == 1
