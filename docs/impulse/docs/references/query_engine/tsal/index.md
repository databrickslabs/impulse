---
title: TSAL
---

# TSAL — Time Series Analytics Language

TSAL is the expression language at the core of Impulse. It provides a Pythonic, Matlab-style syntax for
selecting physical channels, defining virtual signals, and expressing event conditions. All TSAL
expressions are **lazy** — no computation happens until a solver executes the query.

## The flow

Working with Impulse follows one path, and this section is organized around it:

1. **Define expressions.** Select channels by their metadata tags and combine them with operators and
   signal methods to describe signals and events — see [Defining Expressions](defining_expressions.md).
2. **Evaluate.** A query is solved per container: each expression's lazy tree is resolved against the
   silver-layer data and evaluated bottom-up — see [Evaluation](evaluation.md).
3. **Get core-model results.** Each expression resolves to one of a few in-memory result classes
   (`SampleSeries`, `Intervals`, `PointsInTime`, `PointsInTimeSeries`) — the
   [Core Data Model](core_data_model.md).

```python
db = my_report.get_db()

# 1. select physical channels by tags
eng_rpm = db.query.channel(channel_name='Engine RPM', brand='Seat', model='Leon')
amb_air_temp = db.query.channel(channel_name='Ambient Air Temperature')
intake_air_temp = db.query.channel(channel_name='Intake Air Temperature')

# 2. derive a virtual signal
avg_temp = (amb_air_temp + intake_air_temp) / 2

# 3. define an event as a boolean expression over signals
high_rpm = (eng_rpm > 2000) & (eng_rpm < 5000)
```

Nothing above touches Spark or runs a computation — the expressions are evaluated only when the query
is solved.

## In this section

- **[Defining Expressions](defining_expressions.md)** — channel selection, operators, signal methods,
  and virtual signals.
- **[Core Data Model](core_data_model.md)** — the in-memory classes expressions resolve to, and how
  they interact.
- **[Evaluation](evaluation.md)** — the expression tree and how `solve()` turns it into core-model
  results.
