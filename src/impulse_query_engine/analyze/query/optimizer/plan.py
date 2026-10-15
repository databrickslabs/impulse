"""The outcome of query optimization: which subtrees Spark computes ahead of the UDF."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Column

from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesExpression

from .pushdown import RAW_VARIANT, Pushdown, PushdownRule
from .traversal import match_node


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
        """Group the variants by the rule that computes them.

        Raises
        ------
        ValueError
            If two different rules share a name: their pipeline columns would
            collide.
        """
        groups: dict[str, tuple[PushdownRule, dict[int, Pushdown]]] = {}
        for variant, pushdown in self.pushdowns_by_variant.items():
            rule = pushdown.rule
            group = groups.setdefault(rule.name, (rule, {}))
            if group[0] is not rule:
                raise ValueError(f"two different pushdown rules are named {rule.name!r}")
            group[1][variant] = pushdown
        return list(groups.values())

    def lookup(self, node: TimeSeriesExpression) -> tuple[Pushdown, int] | None:
        """Return ``(pushdown, variant)`` when *node* is a pushed-down subtree."""
        pushdown = match_node(node, self.rules)
        if pushdown is None:
            return None
        variant = self.variant_ids.get(pushdown.variant_key)
        if variant in self.variants_by_selector.get(pushdown.selector.selector_id, ()):
            return pushdown, variant
        return None

    def native_targets(
        self, selections: Iterable[Any]
    ) -> dict[int, dict[int, list[tuple[int, bool]]]] | None:
        """Sources of calculated channels that are pushed down as a whole.

        Parameters
        ----------
        selections : Iterable
            The selections of a ``solve_calculated_channels`` call.

        Returns
        -------
        dict or None
            ``{variant: {selector_id: [(channel_id, uses_alias), ...]}}``: the
            calculated channels (by output ``channel_id``) whose rows are the
            given variant's rows of the channel resolved for ``selector_id``.
            ``None`` unless every selection is a ``CalculatedChannel`` whose
            whole expression is a pushdown emitting its result rows.
        """
        from impulse_query_engine.analyze.query.channels.calculated_channel import (
            CalculatedChannel,
        )

        selections = list(selections)
        if not selections:
            return None
        targets: dict[int, dict[int, list[tuple[int, bool]]]] = {}
        for selection in selections:
            if not isinstance(selection, CalculatedChannel):
                return None
            hit = self.lookup(selection.expr)
            if hit is None or not hit[0].rule.emits_result_rows(hit[0]):
                return None
            pushdown, variant = hit
            by_selector = targets.setdefault(variant, {})
            by_selector.setdefault(pushdown.selector.selector_id, []).append(
                (selection.channel_id, pushdown.selector.uses_alias)
            )
        return targets

    def channel_variants(self, selector_ids: Column) -> Column:
        """Variant ids a matched channel ships, given its ``selector_ids`` array.

        A channel ships only its raw data (``[0]``) unless every one of its
        selectors is pushed down; then it ships the derived variants instead.
        """
        ids = F.filter(
            F.transform(selector_ids, lambda sid: sid.cast("long")), lambda sid: sid.isNotNull()
        )
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
        no_variants = F.array().cast("array<int>")
        derived = F.array_distinct(
            F.flatten(
                F.transform(
                    ids, lambda sid: F.coalesce(F.try_element_at(mapping, sid), no_variants)
                )
            )
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
