"""
hierarchy/hierarchy_builder.py — Builds a Category → Brand → Model → Variant taxonomy tree.

Input: list of SKURecords from the registry
Output: list of root HierarchyNode objects (one per category)

Tree structure:
  HierarchyNode(type="category", name="smartphones")
    └── HierarchyNode(type="brand", name="samsung")
          └── HierarchyNode(type="model", name="galaxy-s24")
                └── HierarchyNode(type="variant", name="8gb-128gb-phantom-black", sku_id="...")
"""

from __future__ import annotations

from collections import defaultdict

from models import HierarchyNode, SKURecord


class HierarchyBuilder:
    """
    Builds a hierarchical taxonomy tree from SKU records.
    """

    def build(self, skus: list[SKURecord]) -> list[HierarchyNode]:
        """
        Build and return root-level category nodes.

        Returns:
            List of HierarchyNode objects (one per unique category).
        """
        # category → brand → model → [variant SKUs]
        tree: dict[str, dict[str, dict[str, list[SKURecord]]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(list))
        )

        for sku in skus:
            category = sku.category or "uncategorized"
            brand    = sku.brand or "unknown"
            model    = sku.model or "unknown-model"
            tree[category][brand][model].append(sku)

        roots: list[HierarchyNode] = []
        for category, brands in sorted(tree.items()):
            cat_node = HierarchyNode(
                name=category,
                node_type="category",
                slug=self._slug(category),
                data={"brand_count": len(brands)},
            )
            for brand, models in sorted(brands.items()):
                brand_node = HierarchyNode(
                    name=brand,
                    node_type="brand",
                    slug=self._slug(brand),
                    data={"model_count": len(models)},
                )
                for model, sku_list in sorted(models.items()):
                    model_node = HierarchyNode(
                        name=model,
                        node_type="model",
                        slug=self._slug(model),
                        data={"variant_count": len(sku_list)},
                    )
                    for sku in sorted(sku_list, key=lambda s: s.sku_id):
                        variant_name = sku.variant or "base"
                        variant_node = HierarchyNode(
                            name=variant_name,
                            node_type="variant",
                            slug=self._slug(variant_name),
                            sku_id=sku.sku_id,
                            data={
                                "price_min":           sku.price_min,
                                "price_max":           sku.price_max,
                                "price_currency":      sku.price_currency,
                                "rating_avg":          sku.rating_avg,
                                "rating_count_total":  sku.rating_count_total,
                                "source_urls":         sku.source_urls,
                                "source_domains":      sku.source_domains,
                            },
                        )
                        model_node.children.append(variant_node)
                    brand_node.children.append(model_node)
                cat_node.children.append(brand_node)
            roots.append(cat_node)

        return roots

    @staticmethod
    def _slug(text: str) -> str:
        import re
        return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


hierarchy_builder = HierarchyBuilder()
