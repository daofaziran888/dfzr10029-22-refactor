"""DAG 拓扑依赖的统一解析。

重构前，分析管道、分析模型、数据集市三个 DAG 服务各自写了一遍
「遍历行 -> 组装节点 -> 相邻节点连边」的逻辑，节点详情、导出、运行入口
拿到的边顺序没有共同来源。这里把重复的拓扑遍历收拢成一次依赖解析：

- ``build_dag_graph`` 保留历史拼装口径（节点字段、name 取值、相邻连边顺序），
  保证既有节点输入输出和最终数据结果不变；
- ``TopologyResolver`` 按模块缓存唯一一份解析结果（节点、边、节点依赖表、
  拓扑次序、版本号），页面、导出、运行入口共用同一份拓扑；
- 解析结果带版本号：行数据结构变化时版本递增，管道中间产物记录按同一版本
  回查，跨版本记录保留版本边界，不会被新拓扑覆盖。
"""
from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field
from typing import Any

from app.store import store

# 行内只服务于列表状态、不属于节点业务字段的内部键。
_INTERNAL_KEYS = ("id", "status", "pending", "abnormal")
_DEPENDENCY_LABEL = "依赖"


def build_dag_graph(rows: list[dict[str, Any]], default_status: str) -> dict[str, Any]:
    """按历史口径把模块行一次性解析成 ``{"nodes", "edges"}``。

    节点顺序、字段集合、``name`` 的取值方式以及相邻节点之间的连边顺序，
    均与重构前三个 DAG 服务中的分散实现保持一致。
    """
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        node: dict[str, Any] = {
            "id": int(row.get("id", i + 1)),
            "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
            "status": str(row.get("status", default_status)),
        }
        for key in row:
            if key not in _INTERNAL_KEYS:
                node[key] = row[key]
        nodes.append(node)
    # 一次遍历同时完成节点组装与相邻依赖边的解析，避免重复拓扑遍历。
    for i in range(len(nodes) - 1):
        edges.append({"from": nodes[i]["id"], "to": nodes[i + 1]["id"], "label": _DEPENDENCY_LABEL})
    return {"nodes": nodes, "edges": edges}


def _structural_signature(rows: list[dict[str, Any]]) -> str:
    """计算节点结构指纹：只随节点及其业务字段变化，状态流转不产生新版本。"""
    digest = hashlib.sha1()
    for row in rows:
        digest.update(repr((
            row.get("id"),
            tuple((key, row[key]) for key in row if key not in ("status", "pending", "abnormal")),
        )).encode("utf-8"))
        digest.update(b"|")
    return digest.hexdigest()


@dataclass(frozen=True)
class ResolvedTopology:
    """一次依赖解析落成的统一管道依赖结果。"""

    module: str
    version: int
    signature: str
    order: list[int] = field(default_factory=list)
    nodes: list[dict[str, Any]] = field(default_factory=list)
    edges: list[dict[str, Any]] = field(default_factory=list)
    dependencies: dict[int, list[int]] = field(default_factory=dict)
    dependents: dict[int, list[int]] = field(default_factory=dict)

    def graph(self) -> dict[str, Any]:
        # 返回拷贝，避免调用方改到缓存里的唯一拓扑结果。
        return {
            "nodes": [dict(node) for node in self.nodes],
            "edges": [dict(edge) for edge in self.edges],
        }


class TopologyResolver:
    """按模块维护版本化的拓扑解析缓存，收拢所有 DAG 依赖计算入口。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: dict[str, ResolvedTopology] = {}

    def resolve(self, module: str, default_status: str) -> ResolvedTopology:
        rows = store.rows(module)
        signature = _structural_signature(rows)
        with self._lock:
            cached = self._cache.get(module)
            if cached is not None and cached.signature == signature:
                return cached
            graph = build_dag_graph(rows, default_status)
            version = cached.version + 1 if cached is not None else 1
            dependencies: dict[int, list[int]] = {}
            dependents: dict[int, list[int]] = {node["id"]: [] for node in graph["nodes"]}
            for node in graph["nodes"]:
                dependencies[node["id"]] = []
            for edge in graph["edges"]:
                dependencies[edge["to"]].append(edge["from"])
                dependents[edge["from"]].append(edge["to"])
            resolved = ResolvedTopology(
                module=module,
                version=version,
                signature=signature,
                order=[node["id"] for node in graph["nodes"]],
                nodes=graph["nodes"],
                edges=graph["edges"],
                dependencies=dependencies,
                dependents=dependents,
            )
            self._cache[module] = resolved
            return resolved

    def version_of(self, module: str, default_status: str) -> int:
        return self.resolve(module, default_status).version


topology_resolver = TopologyResolver()
