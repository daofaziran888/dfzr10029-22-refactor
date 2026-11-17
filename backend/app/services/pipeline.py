"""分析管道 DAG 图服务：节点流转规则与统一拓扑依赖解析。

拓扑遍历已收拢到 ``app.services.dag_topology``，本服务只保留分析管道自己的
登记、动作规则，并通过 ``ArtifactRegistry`` 复用统一的中间产物记录实现。
"""
from __future__ import annotations

from typing import Any

from app.services.dag_topology import topology_resolver
from app.services.pipeline_artifacts import artifact_registry
from app.store import store

MODULE = "pipeline"
REQUIRED_FIELDS = ["节点编号", "输入数据集", "分析算子"]
STATUS_ORDER = ["草稿", "已配置", "运行中", "已完成", "失败"]
ACTION_RULES = {"提交配置": "已配置", "启动节点": "运行中", "标记失败": "失败"}
NEGATIVE_ACTIONS = []


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class PipelineService:
    def get_graph(self) -> dict[str, Any]:
        # 页面、导出、运行入口共用同一份版本化拓扑解析结果。
        return topology_resolver.resolve(MODULE, STATUS_ORDER[0]).graph()

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id",0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"分析节点 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于分析管道可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"分析节点已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]

    # ---- 管道运行与中间产物记录：统一走 ArtifactRegistry ---------------------

    def run_pipeline(self, *, force_recompute: bool = False) -> dict[str, Any]:
        return artifact_registry.record_run(force_recompute=force_recompute)

    def list_runs(self) -> list[dict[str, Any]]:
        return artifact_registry.list_runs()

    def list_artifacts(self, topology_version: int | None = None) -> list[dict[str, Any]]:
        return artifact_registry.list_artifacts(topology_version)
