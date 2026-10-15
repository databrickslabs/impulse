"""Rule-based optimizer choosing the subtrees Spark computes ahead of the pandas UDF."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence
from typing import Any

from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesExpression

from .plan import PushdownPlan
from .pushdown import Pushdown, PushdownContext, PushdownRule, check_name
from .resample_rule import ResamplePushdownRule
from .traversal import discover


def default_rules() -> tuple[PushdownRule, ...]:
    """All built-in pushdown rules."""
    return (ResamplePushdownRule(),)


class QueryOptimizer:
    """Plans which subtrees of a solve's selections are computed natively in Spark.

    All selections of a solve share the raw data of their physical channels, so
    a subtree is pushed down only when **every** occurrence of its input selector,
    across all selections, sits inside a pushed-down subtree.  Then the raw rows
    of that selector are no longer needed and are replaced by the (smaller)
    derived rows.  Selectors used anywhere else keep their raw data, and their
    subtrees are evaluated in pandas as before.

    Parameters
    ----------
    rules : Sequence[PushdownRule], optional
        Rules to apply, in priority order.  Defaults to :func:`default_rules`.
    """

    def __init__(self, rules: Sequence[PushdownRule] | None = None):
        self.rules: tuple[PushdownRule, ...] = (
            tuple(rules) if rules is not None else default_rules()
        )
        names = [check_name("rule", rule.name) for rule in self.rules]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"pushdown rule names must be unique: {duplicates}")

    def optimize(self, selections: Iterable[Any], context: PushdownContext) -> PushdownPlan:
        """Build the pushdown plan for the *selections* of one solve call.

        Parameters
        ----------
        selections : Iterable
            The query's selections.
        context : PushdownContext
            Plan-time facts about the channel data.

        Returns
        -------
        PushdownPlan
            The plan; empty when nothing can be pushed down.
        """
        selections = list(selections)
        if not all(isinstance(s, TimeSeriesExpression) for s in selections):
            return PushdownPlan(rules=self.rules)

        total: Counter[int] = Counter()
        candidates: list[tuple[TimeSeriesExpression, Pushdown]] = []
        for selection in selections:
            expected = Counter(s.selector_id for s in selection.get_selectors())
            total.update(expected)
            found, leaves = discover(selection, self.rules)
            walked = Counter(s.selector_id for s in leaves)
            walked.update(s.selector_id for _, p in found for s in p.consumed)
            # A walk that disagrees with get_selectors() missed or double-counted part
            # of the tree; its candidates can't be trusted to cover all occurrences.
            if walked == expected:
                candidates.extend(found)

        candidates = [
            (node, pushdown)
            for node, pushdown in candidates
            if pushdown.rule.supports(pushdown, context) and _has_result_type(node, pushdown)
        ]

        while True:
            consumed = Counter(s.selector_id for _, p in candidates for s in p.consumed)
            fully_pushed = {sid for sid, count in consumed.items() if count == total[sid]}
            kept = [
                (node, pushdown)
                for node, pushdown in candidates
                if all(s.selector_id in fully_pushed for s in pushdown.consumed)
            ]
            if len(kept) == len(candidates):
                break
            candidates = kept

        return PushdownPlan.build(self.rules, [p for _, p in candidates], total)


def _has_result_type(node: TimeSeriesExpression, pushdown: Pushdown) -> bool:
    """Whether *node* evaluates to the type the rule decodes into."""
    try:
        actual = node.evaluation_type()
    except Exception:
        # Only disqualifies the candidate; the query's own type inference reports errors.
        return False
    return actual is pushdown.rule.result_type(pushdown)
