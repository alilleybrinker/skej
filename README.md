# skej

A little analyzer for concurrent schedules.

## Concept

The basic idea of `skej` is that you can give it a description of a schedule,
with reads and writes of some abstract data across different transactions, and
it identifies conflicts, anomalies, constraints, and compatibility with different
isolation models.
