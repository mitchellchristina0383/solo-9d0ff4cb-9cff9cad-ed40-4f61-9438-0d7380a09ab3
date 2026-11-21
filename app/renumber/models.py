"""换号工单的请求 / 响应模型与单记录输入校验。

跨记录校验（网段重叠、池内归属、占用冲突等）在 store 层的事务内完成，
模型层只负责字段形态与 IPv4 字面量合法性。
"""

from __future__ import annotations

import ipaddress
from typing import Literal

from pydantic import BaseModel, Field, field_validator


def _check_ipv4(value: str) -> str:
    try:
        ip = ipaddress.ip_address(value)
    except ValueError as exc:
        raise ValueError(f"非法 IPv4 地址：{value}") from exc
    if ip.version != 4:
        raise ValueError(f"仅支持 IPv4 地址：{value}")
    return value


class RegistryLink(BaseModel):
    """一条点对点链路的预登记信息：两端名称与现用地址（同属一个 /31）。"""

    link_id: str = Field(..., description="链路标识")
    endpoint_a: str = Field(..., description="A 端设备 / 接口名称")
    endpoint_b: str = Field(..., description="B 端设备 / 接口名称")
    addr_a: str = Field(..., description="A 端现用 IPv4 地址")
    addr_b: str = Field(..., description="B 端现用 IPv4 地址")

    @field_validator("link_id", "endpoint_a", "endpoint_b")
    @classmethod
    def _non_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("标识不能为空")
        return v.strip()

    @field_validator("addr_a", "addr_b")
    @classmethod
    def _ipv4(cls, v: str) -> str:
        return _check_ipv4(v)


class RegistryRequest(BaseModel):
    """登记地址池与全部链路的现用 /31 地址（仅空库可登记一次）。"""

    pool: str = Field(..., description="地址池 IPv4 CIDR，如 10.10.0.0/28；掩码长度不大于 31")
    links: list[RegistryLink] = Field(..., min_length=1, description="链路现用地址台账")

    @field_validator("pool")
    @classmethod
    def _pool_shape(cls, v: str) -> str:
        try:
            net = ipaddress.ip_network(v, strict=True)
        except ValueError as exc:
            raise ValueError(f"非法地址池 CIDR（主机位必须为零）：{v}") from exc
        if net.version != 4:
            raise ValueError(f"仅支持 IPv4 地址池：{v}")
        if net.prefixlen > 31:
            raise ValueError("地址池至少要容纳一个 /31，掩码长度不能大于 31")
        return v


class WorkOrderRequest(BaseModel):
    """凭工单号为一条链路领取空闲 /31。"""

    work_order_id: str = Field(..., min_length=1, description="工单号，全局唯一")
    link_id: str = Field(..., min_length=1, description="目标链路标识")

    @field_validator("work_order_id", "link_id")
    @classmethod
    def _non_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("标识不能为空")
        return v.strip()


class ReceiptRequest(BaseModel):
    """单端回执：撤下的旧址与启用的新址，必须与工单分配完全一致才确认。"""

    endpoint: Literal["a", "b"] = Field(..., description="回报端点，a 或 b")
    removed_addr: str = Field(..., description="该端撤下的旧 IPv4 地址")
    activated_addr: str = Field(..., description="该端启用的新 IPv4 地址")

    @field_validator("removed_addr", "activated_addr")
    @classmethod
    def _ipv4(cls, v: str) -> str:
        return _check_ipv4(v)


class EndpointProgress(BaseModel):
    name: str = Field(..., description="端点名称")
    confirmed: bool = Field(..., description="该端回执是否已确认")


class WorkOrderResponse(BaseModel):
    work_order_id: str
    link_id: str
    status: str = Field(..., description="open（等待回执）/ completed（两端确认完毕）")
    old_subnet: str
    new_subnet: str
    old_addr_a: str
    old_addr_b: str
    new_addr_a: str
    new_addr_b: str
    endpoint_a: EndpointProgress
    endpoint_b: EndpointProgress
    replayed: bool = Field(..., description="true 表示原工单重提，返回的是原分配")
    created_at: str
    completed_at: str | None = None


class ReceiptResponse(WorkOrderResponse):
    already_confirmed: bool = Field(..., description="true 表示该端回执为重复 / 迟到件，未重复落账")


class LinkView(BaseModel):
    link_id: str
    endpoint_a: str
    endpoint_b: str
    current_subnet: str = Field(..., description="链路台账上的现用 /31")
    addr_a: str
    addr_b: str


class SubnetView(BaseModel):
    seq: int = Field(..., description="/31 在池内的顺序号（分配按 seq 升序取空闲）")
    subnet: str
    status: str = Field(..., description="free / in_use / reserved")
    link_id: str | None = Field(None, description="in_use 时对应的现用链路")
    work_order_id: str | None = Field(None, description="reserved 时对应的未完工单")


class PoolResponse(BaseModel):
    pool: str
    total_subnets: int
    free: int
    in_use: int
    reserved: int
    subnets: list[SubnetView]


class RegistryResponse(BaseModel):
    initialized: bool
    pool: str | None = None
    total_subnets: int | None = None
    links: list[LinkView] = []
