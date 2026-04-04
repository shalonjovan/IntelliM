"""
hierarchy/hierarchy_exporter.py — Exports the taxonomy tree to JSON and flat JSONL.

Outputs:
  hierarchy.json         — nested tree (Category → Brand → Model → Variant)
  hierarchy_flat.jsonl   — one record per leaf (variant) node for DB ingestion
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import orjson
from loguru import logger

from models import HierarchyNode


class HierarchyExporter:

    def export(
        self,
        roots: list[HierarchyNode],
        output_dir: Path,
    ) -> dict[str, int]:
        """
        Export hierarchy tree to JSON + flat JSONL.

        Returns:
            {"nodes": N, "leaves": M}
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        # 1. Nested JSON
        tree_path = output_dir / "hierarchy.json"
        tree_data = {
            "version": "1.0",
            "categories": [self._node_to_dict(r) for r in roots],
        }
        tree_path.write_bytes(orjson.dumps(tree_data, option=orjson.OPT_INDENT_2))
        logger.info(f"Wrote hierarchy tree to {tree_path}")

        # 2. Flat JSONL (one record per leaf)
        flat_path = output_dir / "hierarchy_flat.jsonl"
        leaves: list[dict[str, Any]] = []
        for root in roots:
            self._collect_leaves(root, path=[], leaves=leaves)

        with open(flat_path, "wb") as fh:
            for leaf in leaves:
                fh.write(orjson.dumps(leaf) + b"\n")
        logger.info(f"Wrote {len(leaves)} leaf nodes to {flat_path}")

        total_nodes = sum(self._count_nodes(r) for r in roots)
        return {"nodes": total_nodes, "leaves": len(leaves)}

    # ------------------------------------------------------------------ private

    def _node_to_dict(self, node: HierarchyNode) -> dict[str, Any]:
        d: dict[str, Any] = {
            "name":      node.name,
            "type":      node.node_type,
            "slug":      node.slug,
        }
        if node.sku_id:
            d["sku_id"] = node.sku_id
        if node.data:
            d["data"] = node.data
        if node.children:
            d["children"] = [self._node_to_dict(c) for c in node.children]
        return d

    def _collect_leaves(
        self,
        node: HierarchyNode,
        path: list[tuple[str, str]],     # (node_type, name) breadcrumb
        leaves: list[dict[str, Any]],
    ) -> None:
        current_path = [*path, (node.node_type, node.name)]

        if not node.children:
            # Leaf node — emit a flat record
            breadcrumb = {ntype: name for ntype, name in current_path}
            leaf = {
                "sku_id":   node.sku_id,
                "category": breadcrumb.get("category", ""),
                "brand":    breadcrumb.get("brand", ""),
                "model":    breadcrumb.get("model", ""),
                "variant":  breadcrumb.get("variant", node.name),
                "slug":     node.slug,
                **node.data,
            }
            leaves.append(leaf)
            return

        for child in node.children:
            self._collect_leaves(child, current_path, leaves)

    def _count_nodes(self, node: HierarchyNode) -> int:
        return 1 + sum(self._count_nodes(c) for c in node.children)


hierarchy_exporter = HierarchyExporter()
