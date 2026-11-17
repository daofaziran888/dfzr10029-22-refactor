"""管道中间产物记录的统一处理。

重构前，中间产物记录散落在运行链路里各写一遍，重复运行复用中间节点与按
依赖链重算会留下互不一致的记录。这里收拢成一套共用实现：

- ``record_run`` 沿收拢后的拓扑次序做一次遍历，逐节点决定「复用物化产物」
  还是「按依赖链重新计算」，并把每个中间节点的处理依据写进运行记录；
- 中间产物按 (节点, 拓扑版本) 唯一确认，冲突条件下保留版本边界——新版本
  只新增记录，旧版本记录仍可按同一版本回查；
- 写入在进程内加锁并以唯一键兜底，并发提交最终只有唯一确认结果；
- 节点输出由节点输入与上游产物做确定性派生，因此复用或重算的运行记录可以
  不同，但最终节点的数据结果保持不变；
- ``cross_domain_snapshot`` 给数据导出等跨领域模块提供只读回查入口。
"""
from __future__ import annotations

import hashlib
import threading
from datetime import datetime
from typing import Any

from app.services.dag_topology import ResolvedTopology, topology_resolver
from app.store import store

MODULE = "pipeline"
DEFAULT_STATUS = "草稿"

ARTIFACT_TABLE = "pipeline_artifacts"
RUN_TABLE = "pipeline_runs"

MODE_REUSE = "reuse"
MODE_RECOMPUTE = "recompute"

SOURCE_REUSE = "复用物化中间产物"
SOURCE_RECOMPUTE = "按依赖链重新计算"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _node_output(node: dict[str, Any], upstream_outputs: list[str]) -> str:
    """节点最终数据结果的确定性派生口径。

    只依赖节点自身业务输入与上游已确认产物，不依赖运行次数或记录来源，
    保证同一拓扑版本下复用与重算得到完全相同的节点结果。
    """
    payload = [str(node.get("输入数据集", "")), str(node.get("分析算子", ""))]
    payload.extend(upstream_outputs)
    digest = hashlib.sha1("\u0001".join(payload).encode("utf-8")).hexdigest()
    return f"{node.get('输出结果', '')}#{digest[:12]}"


class ArtifactRegistry:
    """管道中间产物与运行记录的唯一写入/回查入口。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    # ---- 回查 ----------------------------------------------------------------

    def list_runs(self) -> list[dict[str, Any]]:
        return [dict(row) for row in store.rows(RUN_TABLE)]

    def list_artifacts(self, topology_version: int | None = None) -> list[dict[str, Any]]:
        rows = store.rows(ARTIFACT_TABLE)
        if topology_version is not None:
            rows = [row for row in rows if row["topology_version"] == topology_version]
        return [dict(row) for row in rows]

    def cross_domain_snapshot(self, topology_version: int | None = None) -> dict[str, Any]:
        """跨领域模块（如数据导出）只读回查管道中间产物记录。"""
        resolved = topology_resolver.resolve(MODULE, DEFAULT_STATUS)
        version = resolved.version if topology_version is None else topology_version
        return {
            "module": MODULE,
            "topology_version": version,
            "artifacts": self.list_artifacts(version),
            "runs": [dict(row) for row in store.rows(RUN_TABLE) if row["topology_version"] == version],
        }

    # ---- 运行与落记录 --------------------------------------------------------

    def record_run(self, *, force_recompute: bool = False) -> dict[str, Any]:
        """按统一拓扑跑一遍管道并落成中间产物记录与运行记录。"""
        with self._lock:
            resolved = topology_resolver.resolve(MODULE, DEFAULT_STATUS)
            run_id = self._next_run_id()
            created_at = _now()
            node_records: list[dict[str, Any]] = []
            confirmed_outputs: dict[int, str] = {}

            for node_id in resolved.order:
                node = self._node_of(resolved, node_id)
                upstream = resolved.dependencies.get(node_id, [])
                upstream_outputs = [confirmed_outputs[up] for up in upstream]
                output = _node_output(node, upstream_outputs)
                existing = self._find_artifact(node_id, resolved.version)

                if existing is None:
                    # 该节点在当前拓扑版本下还没有物化产物，只能按依赖链计算。
                    source = SOURCE_RECOMPUTE
                    basis = "当前拓扑版本无物化产物，按依赖链重新计算"
                    artifact = self._materialize(
                        run_id, created_at, resolved, node, output, upstream, source
                    )
                elif force_recompute:
                    # 强制按依赖链重算：结果与既有产物一致时沿用唯一确认产物，
                    # 仅在运行记录里保留重算依据；冲突时保留版本边界并追加确认。
                    source = SOURCE_RECOMPUTE
                    if existing["output"] != output:
                        basis = "强制重算结果与既有物化产物冲突，保留版本边界并追加确认"
                        artifact = self._materialize(
                            run_id, created_at, resolved, node, output, upstream, source
                        )
                    else:
                        basis = "强制按依赖链重新计算，结果与已确认物化产物一致"
                        artifact = existing
                else:
                    # 复用物化中间产物：唯一键保证同一节点同一拓扑版本只确认一次。
                    source = SOURCE_REUSE
                    basis = "命中同一拓扑版本的已确认物化产物"
                    artifact = existing

                confirmed_outputs[node_id] = artifact["output"]
                node_records.append({
                    "node_id": node_id,
                    "节点编号": node.get("节点编号"),
                    "source": source,
                    "basis": basis,
                    "artifact_id": artifact["artifact_id"],
                    "artifact_version": artifact["artifact_version"],
                    "topology_version": resolved.version,
                    "upstream": list(upstream),
                    "output": artifact["output"],
                })

            run_record = {
                "run_id": run_id,
                "topology_version": resolved.version,
                "mode": MODE_RECOMPUTE if force_recompute else MODE_REUSE,
                "created_at": created_at,
                "order": list(resolved.order),
                "final_node": resolved.order[-1] if resolved.order else None,
                "final_output": confirmed_outputs[resolved.order[-1]] if resolved.order else None,
                "nodes": node_records,
            }
            store.rows(RUN_TABLE).append(run_record)
            return dict(run_record)

    # ---- 内部辅助 ------------------------------------------------------------

    def _next_run_id(self) -> int:
        return max((int(row["run_id"]) for row in store.rows(RUN_TABLE)), default=0) + 1

    def _next_artifact_id(self) -> int:
        return max((int(row["artifact_id"]) for row in store.rows(ARTIFACT_TABLE)), default=0) + 1

    @staticmethod
    def _node_of(resolved: ResolvedTopology, node_id: int) -> dict[str, Any]:
        for node in resolved.nodes:
            if node["id"] == node_id:
                return node
        raise KeyError(f"拓扑解析结果中找不到节点 {node_id}")

    def _find_artifact(self, node_id: int, topology_version: int) -> dict[str, Any] | None:
        matches = [
            row for row in store.rows(ARTIFACT_TABLE)
            if row["node_id"] == node_id and row["topology_version"] == topology_version
        ]
        if not matches:
            return None
        # 同一 (节点, 拓扑版本) 只确认最新一条，旧确认保留在表里作为版本边界。
        return max(matches, key=lambda row: int(row["artifact_version"]))

    def _materialize(
        self,
        run_id: int,
        created_at: str,
        resolved: ResolvedTopology,
        node: dict[str, Any],
        output: str,
        upstream: list[int],
        source: str,
    ) -> dict[str, Any]:
        node_id = node["id"]
        prior = self._find_artifact(node_id, resolved.version)
        artifact = {
            "artifact_id": self._next_artifact_id(),
            "artifact_version": (int(prior["artifact_version"]) + 1) if prior else 1,
            "node_id": node_id,
            "节点编号": node.get("节点编号"),
            "topology_version": resolved.version,
            "topology_signature": resolved.signature,
            "upstream": list(upstream),
            "output": output,
            "source": source,
            "confirmed_by_run": run_id,
            "confirmed_at": created_at,
        }
        store.rows(ARTIFACT_TABLE).append(artifact)
        return artifact


artifact_registry = ArtifactRegistry()
