"""The outcome of query optimization: which subtrees Spark computes ahead of the UDF."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import Column

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)

from .pushdown import RAW_VARIANT, Pushdown, PushdownRule


@dataclass(frozen=True, eq=False)
class PushedNode:
    """A pushed-down node as the UDF receives it.

    Pickled with the selections, so :attr:`node` and :attr:`selectors` (the selectors
    the rule computes from) are the objects inside them; the UDF finds the node by
    identity.  :attr:`rule` names a built-in rule (:func:`~.query_optimizer.builtin_rules`).
    """

    node: TimeSeriesExpression
    variant: int
    rule: str
    selectors: tuple[TimeSeriesSelector, ...]


@dataclass(frozen=True, eq=False)
class PushdownPlan:
    """Pushdowns selected for one solve call; the UDF receives only :attr:`pushed_nodes`.

    Variant ``0`` is the raw channel data; variants ``1..n`` are derived series,
    one per distinct :attr:`Pushdown.variant_key`.

    Parameters
    ----------
    variant_ids : Mapping[tuple, int]
        Variant key to variant id.
    pushdowns_by_variant : Mapping[int, Pushdown]
        Variant id to a representative pushdown (rule and parameters).
    variants_by_selector : Mapping[int, tuple of int]
        Selector id to the variant ids derived from it.
    raw_selector_ids : frozenset of int
        Selectors whose raw channel data is still required.
    pushed_nodes : tuple of PushedNode
        Each pushed-down node of the selections once; what the UDF receives.
    """

    variant_ids: Mapping[tuple, int] = field(default_factory=dict)
    pushdowns_by_variant: Mapping[int, Pushdown] = field(default_factory=dict)
    variants_by_selector: Mapping[int, tuple[int, ...]] = field(default_factory=dict)
    raw_selector_ids: frozenset[int] = frozenset()
    pushed_nodes: tuple[PushedNode, ...] = ()
    # id(node) -> (pushdown, variant); the plan holds the nodes, so the ids stay valid.
    _by_node: Mapping[int, tuple[Pushdown, int]] = field(default_factory=dict, repr=False)

    @staticmethod
    def build(
        nodes: Iterable[tuple[TimeSeriesExpression, Pushdown]],
        selector_ids: Iterable[int],
    ) -> PushdownPlan:
        """Assign variant ids to the pushdowns of *nodes*, in discovery order.

        Parameters
        ----------
        nodes : Iterable of (TimeSeriesExpression, Pushdown)
            Accepted ``(node, pushdown)`` pairs; each pushdown's selector is pushed
            down entirely.  A repeated node is recorded once.
        selector_ids : Iterable[int]
            Ids of all selectors in the solve.

        Raises
        ------
        ValueError
            If a pushdown computes on another pushdown's result (``source``),
            which the pipeline cannot compute.
        """
        variant_ids: dict[tuple, int] = {}
        pushdowns_by_variant: dict[int, Pushdown] = {}
        variants_by_selector: dict[int, list[int]] = {}
        by_node: dict[int, tuple[Pushdown, int]] = {}
        pushed_nodes: list[PushedNode] = []
        for node, pushdown in nodes:
            if pushdown.source is not None:
                raise ValueError(f"pushdown {pushdown.describe()} has a source")
            key = pushdown.variant_key
            if key not in variant_ids:
                variant_ids[key] = len(variant_ids) + 1
                pushdowns_by_variant[variant_ids[key]] = pushdown
            variant = variant_ids[key]
            variants = variants_by_selector.setdefault(pushdown.selector.selector_id, [])
            if variant not in variants:
                variants.append(variant)
            if id(node) not in by_node:
                by_node[id(node)] = (pushdown, variant)
                pushed_nodes.append(
                    PushedNode(node, variant, pushdown.rule.name, (pushdown.selector,))
                )
        return PushdownPlan(
            variant_ids=variant_ids,
            pushdowns_by_variant=pushdowns_by_variant,
            variants_by_selector={sid: tuple(v) for sid, v in variants_by_selector.items()},
            raw_selector_ids=frozenset(selector_ids) - variants_by_selector.keys(),
            pushed_nodes=tuple(pushed_nodes),
            _by_node=by_node,
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

    def variant_of(self, node: TimeSeriesExpression) -> tuple[Pushdown, int] | None:
        """``(pushdown, variant)`` if the node object *node* is pushed down, else ``None``.

        Matched by identity, not structure.
        """
        return self._by_node.get(id(node))

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
            hit = self.variant_of(selection.expr)
            if hit is None or not hit[0].rule.emits_result_rows(hit[0]):
                return None
            pushdown, variant = hit
            by_selector = targets.setdefault(variant, {})
            by_selector.setdefault(pushdown.selector.selector_id, []).append(
                (selection.channel_id, pushdown.selector.uses_alias)
            )
        return targets

    def native_selections(self, selections: Iterable[Any]) -> list[tuple[Pushdown, int]] | None:
        """The pushdowns computing the selections of a ``solve`` call, if Spark can produce them.

        Parameters
        ----------
        selections : Iterable
            The selections of a ``solve`` call.

        Returns
        -------
        list or None
            ``(pushdown, variant)`` per selection, in order: each selection is a
            pushed-down node as a whole whose rule builds its result in Spark
            (:meth:`PushdownRule.has_native_value`).  ``None`` unless every selection
            is such a node.
        """
        hits = [self.variant_of(selection) for selection in selections]
        if not hits or any(h is None or not h[0].rule.has_native_value(h[0]) for h in hits):
            return None
        return hits

    def channel_variants(self, selector_ids: Column) -> Column:
        """Variant ids a matched channel ships, given its ``selector_ids`` array.

        A channel ships only its raw data (``[0]``) unless every one of its
        selectors is pushed down; then it ships the derived variants instead.
        """
        ids = PushdownPlan.channel_selector_ids(selector_ids)
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

    @staticmethod
    def channel_selector_ids(selector_ids: Column | str) -> Column:
        """A matched channel's distinct, non-null ``selector_ids``, as ``long``.

        Variant tagging (:meth:`channel_variants`) and the native calculated-channel
        rows both read a channel's selectors through this, so they agree on which
        selectors it carries.
        """
        return F.array_distinct(
            F.filter(
                F.transform(selector_ids, lambda sid: sid.cast("long")),
                lambda sid: sid.isNotNull(),
            )
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
