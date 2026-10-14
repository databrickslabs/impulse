"""The outcome of query optimization: which subtrees Spark computes ahead of the UDF."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

import pyspark.sql.functions as F
from pyspark.sql import Column

from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesExpression

from .pushdown import RAW_VARIANT, Pushdown, PushdownRule


@dataclass(frozen=True, eq=False)
class PushdownPlan:
    """Pushdowns selected for one solve call, shipped to the solver and the UDF.

    Variant ``0`` is the raw channel data; variants ``1..n`` are derived series,
    one per distinct :attr:`Pushdown.variant_key`.

    Parameters
    ----------
    rules : tuple of PushdownRule
        Rules used to recognize pushed-down nodes during evaluation.
    variant_ids : Mapping[tuple, int]
        Variant key to variant id.
    pushdowns_by_variant : Mapping[int, Pushdown]
        Variant id to a representative pushdown (rule and parameters).
    variants_by_selector : Mapping[int, tuple of int]
        Selector id to the variant ids derived from it.
    raw_selector_ids : frozenset of int
        Selectors whose raw channel data is still required.
    """

    rules: tuple[PushdownRule, ...] = ()
    variant_ids: Mapping[tuple, int] = field(default_factory=dict)
    pushdowns_by_variant: Mapping[int, Pushdown] = field(default_factory=dict)
    variants_by_selector: Mapping[int, tuple[int, ...]] = field(default_factory=dict)
    raw_selector_ids: frozenset[int] = frozenset()

    @staticmethod
    def build(
        rules: Iterable[PushdownRule],
        pushdowns: Iterable[Pushdown],
        selector_ids: Iterable[int],
    ) -> PushdownPlan:
        """Assign variant ids to *pushdowns*, in discovery order.

        Parameters
        ----------
        rules : Iterable[PushdownRule]
            The optimizer's rules.
        pushdowns : Iterable[Pushdown]
            Accepted pushdowns; each one's selector is pushed down entirely.
        selector_ids : Iterable[int]
            Ids of all selectors in the solve.
        """
        variant_ids: dict[tuple, int] = {}
        pushdowns_by_variant: dict[int, Pushdown] = {}
        variants_by_selector: dict[int, list[int]] = {}
        for pushdown in pushdowns:
            key = pushdown.variant_key
            if key not in variant_ids:
                variant_ids[key] = len(variant_ids) + 1
                pushdowns_by_variant[variant_ids[key]] = pushdown
            variants = variants_by_selector.setdefault(pushdown.selector.selector_id, [])
            if variant_ids[key] not in variants:
                variants.append(variant_ids[key])
        return PushdownPlan(
            rules=tuple(rules),
            variant_ids=variant_ids,
            pushdowns_by_variant=pushdowns_by_variant,
            variants_by_selector={sid: tuple(v) for sid, v in variants_by_selector.items()},
            raw_selector_ids=frozenset(selector_ids) - variants_by_selector.keys(),
        )

    @property
    def is_empty(self) -> bool:
        """``True`` when nothing is pushed down."""
        return not self.variant_ids

    @property
    def pushed_selector_ids(self) -> frozenset[int]:
        """Selectors whose raw channel data is replaced by derived variants."""
        return frozenset(self.variants_by_selector)

    def variants_by_rule(self) -> list[tuple[PushdownRule, dict[int, Pushdown]]]:
        """Group the variants by the rule that computes them."""
        groups: dict[str, tuple[PushdownRule, dict[int, Pushdown]]] = {}
        for variant, pushdown in self.pushdowns_by_variant.items():
            rule = pushdown.rule
            groups.setdefault(rule.name, (rule, {}))[1][variant] = pushdown
        return list(groups.values())

    def lookup(self, node: TimeSeriesExpression) -> tuple[Pushdown, int] | None:
        """Return ``(pushdown, variant)`` when *node* is a pushed-down subtree."""
        for rule in self.rules:
            pushdown = rule.match(node)
            if pushdown is None:
                continue
            variant = self.variant_ids.get(pushdown.variant_key)
            if variant in self.variants_by_selector.get(pushdown.selector.selector_id, ()):
                return pushdown, variant
            return None
        return None

    def channel_variants(self, selector_ids: Column) -> Column:
        """Variant ids a matched channel ships, given its ``selector_ids`` array.

        A channel ships only its raw data (``[0]``) unless every one of its
        selectors is pushed down; then it ships the derived variants instead.
        """
        ids = F.transform(selector_ids, lambda sid: sid.cast("long"))
        pushed = sorted(self.variants_by_selector)
        needs_raw = F.exists(ids, lambda sid: ~sid.isin(pushed)) | (F.size(ids) == 0)
        mapping = F.create_map(
            *[
                item
                for sid in pushed
                for item in (
                    F.lit(sid).cast("long"),
                    F.array(*[F.lit(v) for v in self.variants_by_selector[sid]]),
                )
            ]
        )
        derived = F.array_distinct(
            F.flatten(F.transform(ids, lambda sid: F.try_element_at(mapping, sid)))
        )
        return F.when(F.coalesce(needs_raw, F.lit(True)), F.array(F.lit(RAW_VARIANT))).otherwise(
            derived
        )

    def describe(self) -> str:
        """One-line summary of the plan for logging."""
        variants = ", ".join(
            f"{vid}={pushdown.describe()}" for vid, pushdown in self.pushdowns_by_variant.items()
        )
        return (
            f"variants [{variants}]; pushed selectors {sorted(self.pushed_selector_ids)}; "
            f"raw selectors {sorted(self.raw_selector_ids)}"
        )
