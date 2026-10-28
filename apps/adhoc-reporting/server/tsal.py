import ast
from dataclasses import dataclass

SIGNAL = "signal"
INTERVALS = "intervals"
POINTS = "points"
POINT_VALUES = "point_values"
NUMBER = "number"

LABELS = {
    SIGNAL: "a signal",
    INTERVALS: "a condition (time windows)",
    POINTS: "points in time",
    POINT_VALUES: "values at points in time",
    NUMBER: "a number",
}

MAX_LENGTH = 1000
MAX_NODES = 300

ARITHMETIC = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod)
COMPARISONS = (ast.Gt, ast.GtE, ast.Lt, ast.LtE, ast.Eq, ast.NotEq)

WIDTH = [NUMBER]
METHODS: dict[str, dict[str, tuple[list[str], str]]] = {
    SIGNAL: {
        "resample": (WIDTH, SIGNAL),
        "rolling_average": (WIDTH, SIGNAL),
        "cumtrapz": ([], SIGNAL),
        "sparse": ([], SIGNAL),
        "diff": ([], SIGNAL),
        "rising_edges": ([], POINTS),
        "falling_edges": ([], POINTS),
        "intervals_between_falling_edges": ([], INTERVALS),
    },
    INTERVALS: {
        "merge_overlaps": ([], INTERVALS),
        "merge_intervals": (WIDTH, INTERVALS),
        "debounce": (WIDTH, INTERVALS),
        "filter": (WIDTH, INTERVALS),
        "expand": (WIDTH, INTERVALS),
        "expand_left": (WIDTH, INTERVALS),
        "expand_right": (WIDTH, INTERVALS),
        "shrink": (WIDTH, INTERVALS),
        "shrink_left": (WIDTH, INTERVALS),
        "shrink_right": (WIDTH, INTERVALS),
        "start_points": ([], POINTS),
        "end_points": ([], POINTS),
    },
    POINTS: {
        "expand": (WIDTH, INTERVALS),
        "expand_left": (WIDTH, INTERVALS),
        "expand_right": (WIDTH, INTERVALS),
    },
}


class ExprError(ValueError):
    pass


@dataclass(frozen=True)
class Checked:
    type: str
    code: str
    names: frozenset[str]


class _Checker:
    def __init__(self, signals: set[str]):
        self.signals = signals
        self.used: set[str] = set()

    def visit(self, node: ast.expr) -> tuple[str, ast.expr]:
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
                raise ExprError(f"Only numbers are allowed as constants, not {node.value!r}")
            return NUMBER, node
        if isinstance(node, ast.Name):
            if node.id not in self.signals:
                known = ", ".join(sorted(self.signals)) or "none yet"
                raise ExprError(f"Unknown signal {node.id}. Defined signals: {known}")
            self.used.add(node.id)
            return SIGNAL, node
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
            kind, operand = self.visit(node.operand)
            if kind != NUMBER:
                raise ExprError("A minus sign works on numbers only; write 0 - signal instead")
            return NUMBER, ast.UnaryOp(op=node.op, operand=operand)
        if isinstance(node, ast.BinOp):
            return self.binary(node)
        if isinstance(node, ast.Compare):
            return self.compare(node)
        if isinstance(node, ast.Call):
            return self.call(node)
        if isinstance(node, ast.BoolOp):
            raise ExprError("Combine conditions with & and | instead of and / or, with each condition in parentheses")
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            raise ExprError("not is unsupported; invert the comparison instead")
        raise ExprError(f"Unsupported syntax: {type(node).__name__}")

    def binary(self, node: ast.BinOp) -> tuple[str, ast.expr]:
        left, lnode = self.visit(node.left)
        right, rnode = self.visit(node.right)
        if isinstance(node.op, ARITHMETIC):
            kinds = {left, right}
            if kinds == {NUMBER}:
                kind = NUMBER
            elif kinds <= {SIGNAL, NUMBER}:
                kind = SIGNAL
            elif POINT_VALUES in kinds and kinds <= {POINT_VALUES, NUMBER}:
                kind = POINT_VALUES
            else:
                raise ExprError(f"Arithmetic needs signals or numbers, got {LABELS[left]} and {LABELS[right]}")
            return kind, ast.BinOp(left=lnode, op=node.op, right=rnode)
        if isinstance(node.op, ast.BitAnd):
            if left == right == INTERVALS:
                return INTERVALS, ast.BinOp(left=lnode, op=node.op, right=rnode)
            if {left, right} <= {INTERVALS, POINTS}:
                return POINTS, ast.BinOp(left=lnode, op=node.op, right=rnode)
            raise ExprError(f"& is not defined for {LABELS[left]} and {LABELS[right]}")
        if isinstance(node.op, ast.BitOr):
            if left == right and left in (INTERVALS, POINTS):
                return left, ast.BinOp(left=lnode, op=node.op, right=rnode)
            if left == POINTS and right == INTERVALS:
                return POINTS, ast.BinOp(left=lnode, op=node.op, right=rnode)
            raise ExprError(f"| is not defined for {LABELS[left]} and {LABELS[right]}")
        raise ExprError(f"Unsupported operator: {type(node.op).__name__}")

    def compare(self, node: ast.Compare) -> tuple[str, ast.expr]:
        if len(node.ops) > 1:
            raise ExprError("Chained comparisons are not supported; write (x > a) & (x < b)")
        if not isinstance(node.ops[0], COMPARISONS):
            raise ExprError(f"Unsupported comparison: {type(node.ops[0]).__name__}")
        (left, lnode), (right, rnode) = self.visit(node.left), self.visit(node.comparators[0])
        kinds = {left, right}
        if kinds <= {SIGNAL, NUMBER} and SIGNAL in kinds:
            kind = INTERVALS
        elif POINT_VALUES in kinds and kinds <= {POINT_VALUES, NUMBER}:
            kind = POINTS
        else:
            raise ExprError(f"Comparisons need a signal and a number or two signals, got {LABELS[left]} and {LABELS[right]}")
        return kind, ast.Compare(left=lnode, ops=node.ops, comparators=[rnode])

    def call(self, node: ast.Call) -> tuple[str, ast.expr]:
        if not isinstance(node.func, ast.Attribute):
            raise ExprError("Only signal methods can be called, for example rpm.rising_edges()")
        if node.keywords:
            raise ExprError(f"{node.func.attr}() takes positional arguments only")
        receiver, rnode = self.visit(node.func.value)
        method = node.func.attr
        if receiver == SIGNAL and method == "where":
            if len(node.args) != 1:
                raise ExprError("where() takes one condition or set of points in time")
            arg, anode = self.visit(node.args[0])
            if arg not in (INTERVALS, POINTS):
                raise ExprError(f"where() needs a condition or points in time, got {LABELS[arg]}")
            kind = SIGNAL if arg == INTERVALS else POINT_VALUES
            return kind, ast.Call(func=ast.Attribute(value=rnode, attr=method, ctx=ast.Load()), args=[anode], keywords=[])
        spec = METHODS.get(receiver, {}).get(method)
        if spec is None:
            allowed = ", ".join(sorted([*METHODS.get(receiver, {}), *(["where"] if receiver == SIGNAL else [])])) or "none"
            raise ExprError(f"{method}() is not available on {LABELS[receiver]}. Available: {allowed}")
        params, result = spec
        if len(node.args) != len(params):
            raise ExprError(f"{method}() takes {len(params)} argument{'s' if len(params) != 1 else ''}")
        args = []
        for arg_node, param in zip(node.args, params):
            kind, checked = self.visit(arg_node)
            if kind != param:
                raise ExprError(f"{method}() needs {LABELS[param]}, got {LABELS[kind]}")
            args.append(checked)
        return result, ast.Call(func=ast.Attribute(value=rnode, attr=method, ctx=ast.Load()), args=args, keywords=[])


def check(expr: str, signals: set[str]) -> Checked:
    text = expr.strip()
    if not text:
        raise ExprError("Expression is empty")
    if len(text) > MAX_LENGTH:
        raise ExprError(f"Expression is longer than {MAX_LENGTH} characters")
    try:
        tree = ast.parse(text, mode="eval")
    except SyntaxError as e:
        raise ExprError(f"Syntax error at character {e.offset or 0}: {e.msg}") from None
    if sum(1 for _ in ast.walk(tree)) > MAX_NODES:
        raise ExprError("Expression is too complex")
    checker = _Checker(signals)
    kind, node = checker.visit(tree.body)
    return Checked(kind, ast.unparse(ast.fix_missing_locations(node)), frozenset(checker.used))
