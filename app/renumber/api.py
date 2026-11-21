"""换号工单 HTTP 路由：登记、开单、回执、查询。"""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Response

from .models import (
    PoolResponse,
    ReceiptRequest,
    ReceiptResponse,
    RegistryRequest,
    RegistryResponse,
    WorkOrderRequest,
    WorkOrderResponse,
)
from .store import RenumberError, RenumberStore

router = APIRouter(prefix="/renumber", tags=["IPv4 换号工单"])

_store: RenumberStore | None = None


def get_store() -> RenumberStore:
    """进程内单例；数据文件持久化，重启后同一文件恢复全部状态。"""
    global _store
    if _store is None:
        db_path = os.environ.get("RENUMBER_DB_PATH", "/data/renumber.db")
        _store = RenumberStore(db_path)
    return _store


def _raise(exc: RenumberError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=str(exc))


@router.post(
    "/registry",
    response_model=RegistryResponse,
    status_code=201,
    summary="预登记地址池与各链路两端的现用 /31 地址（仅空库可登记一次）",
)
def post_registry(req: RegistryRequest) -> RegistryResponse:
    try:
        return get_store().register(req)
    except RenumberError as exc:
        raise _raise(exc) from exc


@router.get(
    "/registry",
    response_model=RegistryResponse,
    summary="查询地址池与链路台账",
)
def get_registry() -> RegistryResponse:
    return get_store().get_registry()


@router.post(
    "/work-orders",
    response_model=WorkOrderResponse,
    summary="凭工单号为一条链路领取空闲 /31 及两端新地址（重提返回原分配）",
)
def post_work_order(req: WorkOrderRequest, response: Response) -> WorkOrderResponse:
    try:
        view, created = get_store().create_work_order(req)
    except RenumberError as exc:
        raise _raise(exc) from exc
    # 首次创建 201；原工单重提命中幂等，返回 200 且 replayed=true
    response.status_code = 201 if created else 200
    return view

@router.get(
    "/work-orders/{work_order_id}",
    response_model=WorkOrderResponse,
    summary="按工单号查询分配与两端确认进度",
)
def get_work_order(work_order_id: str) -> WorkOrderResponse:
    try:
        return get_store().get_work_order(work_order_id)
    except RenumberError as exc:
        raise _raise(exc) from exc


@router.post(
    "/work-orders/{work_order_id}/receipts",
    response_model=ReceiptResponse,
    summary="单端回执：回报撤下的旧址与启用的新址，地址对得上才确认",
)
def post_receipt(work_order_id: str, req: ReceiptRequest) -> ReceiptResponse:
    try:
        view, _already = get_store().submit_receipt(work_order_id, req)
    except RenumberError as exc:
        raise _raise(exc) from exc
    return view


@router.get(
    "/pool",
    response_model=PoolResponse,
    summary="查看池内全部 /31 的 free / in_use / reserved 状态",
)
def get_pool() -> PoolResponse:
    try:
        return get_store().get_pool()
    except RenumberError as exc:
        raise _raise(exc) from exc
