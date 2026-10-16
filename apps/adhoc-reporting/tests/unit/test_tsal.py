import pytest

from server import tsal

SIGNALS = {"rpm", "speed", "temp"}

TYPES = [
    ("rpm", tsal.SIGNAL),
    ("(rpm + speed) / 2", tsal.SIGNAL),
    ("rpm * 0.10472 - 1", tsal.SIGNAL),
    ("speed.resample(1e6).cumtrapz() / 3600 / 1e6", tsal.SIGNAL),
    ("rpm.rolling_average(5e6)", tsal.SIGNAL),
    ("rpm.where(speed > 0)", tsal.SIGNAL),
    ("rpm % 10", tsal.SIGNAL),
    ("10 - rpm", tsal.SIGNAL),
    ("rpm > 2000", tsal.INTERVALS),
    ("2000 < rpm", tsal.INTERVALS),
    ("rpm > speed", tsal.INTERVALS),
    ("(rpm > 2000) & (rpm < 5000)", tsal.INTERVALS),
    ("(temp < 0) | (temp > 90)", tsal.INTERVALS),
    ("(speed.resample(1e6).cumtrapz() % 10).intervals_between_falling_edges()", tsal.INTERVALS),
    ("(rpm > 3000).filter(2e6).expand(1e6)", tsal.INTERVALS),
    ("rpm.rising_edges().expand(10e6)", tsal.INTERVALS),
    ("rpm.rising_edges()", tsal.POINTS),
    ("(rpm > 2000).start_points()", tsal.POINTS),
    ("(speed > 0) & rpm.falling_edges()", tsal.POINTS),
    ("rpm.rising_edges() | rpm.falling_edges()", tsal.POINTS),
    ("rpm.rising_edges() & (speed > 0)", tsal.POINTS),
    ("rpm.rising_edges() | (speed > 0)", tsal.POINTS),
    ("rpm.where(speed.rising_edges()) > 3000", tsal.POINTS),
    ("rpm.where(speed.rising_edges())", tsal.POINT_VALUES),
    ("-5 * 2", tsal.NUMBER),
]


@pytest.mark.parametrize(("expr", "expected"), TYPES)
def test_types(expr, expected):
    assert tsal.check(expr, SIGNALS).type == expected


def test_used_names():
    assert tsal.check("(rpm + speed) / 2", SIGNALS).names == {"rpm", "speed"}


REJECTED = [
    ("", "empty"),
    ("rpm +", "Syntax error"),
    ("oil", "Unknown signal oil"),
    ("(rpm > 1) and (rpm < 5)", "& and |"),
    ("not rpm > 1", "not is unsupported"),
    ("-rpm", "0 - signal"),
    ("rpm > 'x'", "Only numbers"),
    ("rpm > True", "Only numbers"),
    ("open('x')", "Only signal methods"),
    ("rpm.__class__", "Unsupported syntax: Attribute"),
    ("rpm.__class__()", "not available"),
    ("rpm[0]", "Unsupported syntax: Subscript"),
    ("(lambda: 1)()", "Only signal methods"),
    ("rpm.resample(rate=1)", "positional"),
    ("rpm.resample()", "takes 1 argument"),
    ("rpm.resample(speed)", "needs a number"),
    ("rpm.where(rpm)", "needs a condition"),
    ("rpm.expand(5)", "not available on a signal"),
    ("(rpm > 1) + 1", "Arithmetic needs"),
    ("(rpm > 1) | rpm.rising_edges()", "| is not defined for a condition (time windows) and points in time"),
    ("rpm & speed", "& is not defined"),
    ("1 < 2", "Comparisons need"),
    ("2000 < rpm < 5000", "Chained comparisons are not supported"),
    ("rpm.where(speed.rising_edges()) > speed", "Comparisons need"),
    ("rpm " + "+ rpm " * 400, "longer than"),
]


@pytest.mark.parametrize(("expr", "message"), REJECTED)
def test_rejected(expr, message):
    with pytest.raises(tsal.ExprError) as error:
        tsal.check(expr, SIGNALS)
    assert message in str(error.value)
