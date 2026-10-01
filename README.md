# Day 14 — DataFrame and Dataset

A case class converted to and from a Dataset, RDD vs DataFrame vs Dataset
compared, type safety and Catalyst shown with real `.explain()` plans, and
a typed employee payroll pipeline.

## Run

```bash
sbt "run local[4]"
```

## What it covers

1. **Case class → Dataset**: `employeesDf.as[Employee]` converts an
   untyped `DataFrame` (`Dataset[Row]`) into a typed `Dataset[Employee]`.
   Column names/types are checked against the case class right there.
2. **Dataset → DataFrame**: `employeesDs.toDF()` drops back to the
   untyped column API — useful for joins, pivots, or handing data to code
   that expects a plain `DataFrame`.
3. **RDD vs DataFrame vs Dataset**: a table comparing type safety,
   optimization, ease of use, and best-fit use cases for each.
4. **Type safety + Catalyst**: a deliberate column-name typo on the
   DataFrame API is caught only at runtime (`AnalysisException`); the
   same typo on a Dataset field would be a compile error. Two real
   `.explain()` plans are printed side by side — one from a `Column`
   expression filter (fully inside Catalyst/Tungsten) and one from a
   Scala-lambda filter on the Dataset (wrapped in
   `DeserializeToObject`/`SerializeFromObject`, since a lambda is opaque
   to the optimizer).
5. **Scenario**: `employees.csv` (40 rows) becomes `Dataset[Employee]`,
   a typed `.map` computes gross/net pay into `Dataset[PayrollRecord]`,
   then the aggregation step switches to the column/SQL API (where
   Catalyst does the most good) for a department-level summary, and the
   result is registered as a temp view via `.toDF()`.

## Push to GitHub

```bash
git init && git add . && git commit -m "Day 14: DataFrame and Dataset"
git branch -M main
git remote add origin https://github.com/seemran13/day14-dataframe-dataset.git
git push -u origin main
```
