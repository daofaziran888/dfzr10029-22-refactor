"""数据集市 DAG 图服务：生成节点、边示例结构与流转规则。"""
from __future__ import annotations

from typing import Any

from app.services.dag_topology import topology_resolver
from app.store import store

MODULE = "data_mart"
REQUIRED_FIELDS = ["表编号", "表名称", "主题域"]
STATUS_ORDER = ["草稿", "已建模", "已发布", "已下线"]
ACTION_RULES = {"提交建模": "已建模", "发布集市": "已发布", "下线集市": "已下线"}
NEGATIVE_ACTIONS = ["下线集市"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class DataMartService:
    def get_graph(self) -> dict[str, Any]:
        # 与分析管道、分析模型共用同一套收拢后的拓扑依赖解析。
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
            return None, f"集市表 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据集市可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"集市表已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]
