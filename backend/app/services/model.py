"""分析模型 DAG 图服务：生成节点、边示例结构与流转规则。"""
from __future__ import annotations

from typing import Any

from app.services.dag_topology import topology_resolver
from app.store import store

MODULE = "model"
REQUIRED_FIELDS = ["模型编号", "模型名称", "模型类型"]
STATUS_ORDER = ["草稿", "已训练", "评估中", "已部署", "已下线"]
ACTION_RULES = {"提交训练": "已训练", "启动评估": "评估中", "部署模型": "已部署"}
NEGATIVE_ACTIONS = []


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class ModelService:
    def get_graph(self) -> dict[str, Any]:
        # 与分析管道、数据集市共用同一套收拢后的拓扑依赖解析。
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
            return None, f"模型配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于分析模型可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"模型配置已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]
