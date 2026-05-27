"""Validation for ``SequenceOfEvents`` registrations on a
``MeasurementDB`` configured with ``perception_config``.

Channel-only ``SequenceOfEvents`` is unchanged — sequences over scalar
predicates work exactly as before. The perception concern is: when a
sequence has perception steps, the steps must agree on ``track_scope``.
A sequence that
mixes ``track_scope=True`` and ``track_scope=False`` perception steps is
ambiguous — the chain solver can't simultaneously enforce per-object
identity (which requires per-object cache iteration) and per-container
collapsing (which uses the full cache).

Channel-only steps inside a mixed sequence have no ``track_scope`` axis
and are not constrained. A sequence with only channel steps is exempt.
"""

from __future__ import annotations

from mda_query_engine.perception.tsal.perception_selector import (
    PerceptionSelector,
    perception_selectors_track_scope_values,
)


def validate_sequence_of_events_track_scope(sequence) -> None:
    """Raise if ``sequence``'s perception steps disagree on ``track_scope``.

    ``sequence`` is a ``SequenceOfEvents`` instance from
    ``mda_reporting.events.sequence_of_events``. The function pulls each
    step's selector list off the wrapped ``SequenceOfEventsExpression`` and
    inspects only the perception selectors — channel selectors are exempt.

    No-op when the sequence has zero perception steps (channel-only
    sequence — handled by the core solver unchanged) or when all perception
    selectors agree on a single ``track_scope`` value.
    """
    expression = getattr(sequence, "expression", None)
    if expression is None:
        return
    step_expressions = getattr(expression, "expressions", None)
    if step_expressions is None:
        return

    perception_steps_track_scopes: list[set[bool]] = []
    for step_expr in step_expressions:
        step_values = perception_selectors_track_scope_values(step_expr)
        if step_values:
            perception_steps_track_scopes.append(step_values)
            # Mixing inside a single step is a different invariant —
            # ``PerceptionEvent.__init__`` catches it, but a raw selector
            # tree handed straight to ``SequenceOfEvents`` does not pass
            # through that gate. Guard explicitly here.
            if step_values == {True, False}:
                raise ValueError(
                    f"SequenceOfEvents {sequence.get_name()!r}: step "
                    f"{step_expr} mixes track-scoped and non-track-scoped "
                    "perception selectors. Pick one shape per step."
                )

    if not perception_steps_track_scopes:
        return  # channel-only sequence; the core solver handles it unchanged.

    union: set[bool] = set()
    for values in perception_steps_track_scopes:
        union |= values
    if union == {True, False}:
        raise ValueError(
            f"SequenceOfEvents {sequence.get_name()!r}: perception steps "
            "disagree on track_scope. All perception steps in a sequence "
            "must use the same track_scope value (mixed True/False is "
            "ambiguous)."
        )
