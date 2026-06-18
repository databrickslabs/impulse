---
sidebar_position: 3
title: Evaluation
---

# Evaluation

A TSAL expression is **lazy**: building it constructs a tree of typed nodes and computes nothing. The
tree is evaluated only when `QueryBuilder.solve()` runs, which resolves each expression against the
silver-layer data and produces a [core-model](core_data_model.md) result per container.

## The expression tree

Under the hood, TSAL expressions form a tree of typed nodes:

| Type                      | Role                                                                      |
|---------------------------|---------------------------------------------------------------------------|
| `TimeSeriesSelector`      | Leaf node: selects a physical channel by tag expression.                  |
| `TimeSeriesOp`            | Internal node: arithmetic, comparison, logical, or method-call operation. |
| `TimeSeriesUDF`           | User-defined function applied to one or more expressions.                 |

Operators and signal methods on a `TimeSeriesExpression` ([Defining Expressions](defining_expressions.md))
build `TimeSeriesOp` nodes around their operands rather than computing anything immediately.

## From expression to result

When `QueryBuilder.solve()` runs, the [solver](../query_solvers.md) does the following per container:

1. **Resolve leaves.** Each `TimeSeriesSelector` is matched to a physical channel and loaded into a
   `SampleSeries` from the silver-layer data.
2. **Evaluate bottom-up.** Each `TimeSeriesOp` calls the corresponding method/operator on the
   core-model object its children produced — e.g. `eng_rpm > 2000` builds a `SampleSeries` for
   `eng_rpm`, then the `>` op turns it into an `Intervals`. The result of the whole tree is one
   [core-model](core_data_model.md) object (or a scalar) per container.
3. **Serialize into the output DataFrame.** Each result type maps to a Spark column type:

   | Result type          | Spark column type                  | How it is stored        |
   |----------------------|------------------------------------|-------------------------|
   | `SampleSeries`       | `BinaryType`                       | serialized (pickle+lz4) |
   | `Intervals`          | `ArrayType(ArrayType(DoubleType))` | `[[tstart, tend], ...]` |
   | `PointsInTime`       | `ArrayType(DoubleType)`            | `[tstart, ...]`         |
   | `PointsInTimeSeries` | `ArrayType(ArrayType(DoubleType))` | `[[tstart, value], ...]`|
   | scalar               | `DoubleType`                       | the value               |

`toPandas()` deserializes the binary `SampleSeries` columns back into objects; the array-backed types
are returned as nested lists. See the [Core Data Model](core_data_model.md) for the semantics of each
result class, and [Query Solvers](../query_solvers.md) for the full solver pipeline and the
`DefaultSolver` that reads the silver layer.
