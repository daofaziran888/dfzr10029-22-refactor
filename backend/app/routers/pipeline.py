"""分析管道 DAG 拓扑接口：返回节点、连线图结构。"""
from __future__ import annotations

import csv
import io
import json
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.pipeline import PipelineService

router = APIRouter(prefix="/api/pipeline", tags=["分析管道"])
service = PipelineService()
LIST_FIELDS = ["节点编号", "输入数据集", "分析算子", "输出结果", "执行顺序", "依赖节点", "失败策略", "节点状态"]
STATUSES = ["草稿", "已配置", "运行中", "已完成", "失败"]


class PipelineRunPayload(BaseModel):
    """管道运行载荷：默认优先复用物化中间产物，可要求按依赖链强制重算。"""

    force_recompute: bool = False


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def get_graph() -> dict[str, Any]:
    """返回节点与边组成的 DAG 图数据。"""
    return service.get_graph()


@router.get("/export")
def export_entries() -> StreamingResponse:
    nodes, edges = service.raw_data()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["type", "id", "name", "from", "to", "label", "status"])
    for node in nodes:
        writer.writerow(["node", node.get("id",""), csv_cell(node.get("name","")), "", "", "", node.get("status","")])
    for edge in edges:
        writer.writerow(["edge", "", "", edge.get("from",""), edge.get("to",""), edge.get("label",""), ""])
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=pipeline.csv"})


@router.get("/runs")
def list_runs() -> dict[str, Any]:
    """管道运行记录：可查看每次运行的中间产物复用或重算依据。"""
    return {"items": service.list_runs()}


@router.get("/artifacts")
def list_artifacts(topology_version: int | None = Query(default=None)) -> dict[str, Any]:
    """管道中间产物记录：可按拓扑版本回查同一版本下确认的物化产物。"""
    return {"items": service.list_artifacts(topology_version)}


@router.post("/run")
def run_pipeline(payload: PipelineRunPayload) -> dict[str, Any]:
    """运行入口：依赖解析结果同时服务运行与导出，落成统一的运行与产物记录。"""
    run = service.run_pipeline(force_recompute=payload.force_recompute)
    return {
        "ok": True,
        "message": "分析管道运行完成，中间产物记录已确认",
        "run": run,
    }


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"分析节点 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="分析节点已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
