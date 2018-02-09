To efficiently query MacroBase, we've extended the ANSI SQL standard with our
MacroBase operators, which we call MacroBase SQL. Finding explanations for
anomalies in your data is as simple as issuing a SQL query from the command
line using our custom shell, which enables users to easily explore their data
in an interactive way.

## New SQL Operator: DIFF

To extend MacroBase queries in SQL, we've extend the standard SQL syntax to
include our own operator, called `DIFF`.

### DIFF Inputs

- `DIFF` takes in two relations as arguments: the first relation is the
  **outliers** relation, while the second is the **inliers** relation. This
  represents the classification stage of the MacroBase pipeline. Important:
  both relations must share the same schema!

- The `DIFF` operator also requires you to specify which columns in the given
  table you want to consider for explanations. This is done using the `ON`
  keyword in SQL; `ON location, version, hw_model` means that MacroBase will
  use those three columns for explanation generation. If you want to consider
  all possible columns, use `ON *`; MacroBase SQL will intelligently select the
  proper columns to explore for an initial query.


- You can also specify several optional keywords to customize your query:   
    - `COMPARE BY`, specifies a **ratio metric function**, such as
      `global_ratio`, `risk_ratio`, or `prevalence_ratio`. (`global_ratio` is
      the default.) Users must also specify an **aggregation function** (such
      as `COUNT`, or `AVG`) that takes a column (or multiple columns) as an
      argument.

    - `WITH MIN RATIO` or `WITH MIN SUPPORT`, which specifies the minimum ratio
      and support thresholds for your query. (By default, the minimum ratio is
      1.5, and the minimum support is 0.2)
      
    - `MAX COMBO`, which specifies the maximum order you want for your
      generated explanations (e.g., `MAX COMBO [number]`)

Remember: a `DIFF` query is just SQL! So you can include any other standard
SQL clause: you can add `WHERE` clauses, `ORDER BY`s, and `LIMIT`s, for
example. (`GROUP BY` and `HAVING` is not yet supported.) For example, if you
want to prune out results with low support (e.g., 0.2), simply add `WHERE support > 0.2`
to your SQL query. (By default, DIFF queries prune out all
results with support less than 0.2, and ratios less than 1.5.)

## New SQL Operator: SPLIT

Manually specifying the outliers and inliers for your `DIFF` query can be a pain for users,
especially if the inlier and outlier subqueries are quite redundant. To simplify things, we've
introduced an additional operator called `SPLIT`, which segments a single relation in SQL
into two distinct relations.

The `SPLIT` clause has the following template:
```sql
SPLIT <relation> WHERE <boolean_expression>
```
We can use the `SPLIT` clause in our `DIFF` query by passing it as a single argument to our
`DIFF` operator:

```sql
SELECT * FROM DIFF (SPLIT <relation> WHERE <boolean_expression>)
 ...
 ...
```

Here, tuples where `<boolean_expression>` evaluates to true will be placed in
the outlier table, while ones that evaluate to false will be placed in the
inlier table.

You can also specify a subquery as the argument to your `SPLIT` clause:
```sql
SELECT * FROM DIFF
  (SPLIT
    (SELECT * FROM <relation> WHERE ...)
   WHERE <boolean_expression>)
 ...
 ...
```

## Summary
Overall, the formal definition of a `DIFF` query looks something like this:
```sql
SELECT <column_name>,..., <column_name>
FROM DIFF ([ <relation> | <subquery> ], [ <relation> | <subquery> ])
ON { <column_name>,..., <column_name> | * }
[ WITH MIN SUPPORT <decimal> | MIN RATIO <decimal> ]
[ COMPARE BY { <ratio_metric_fn>(<aggregation_fn>(<column_name> | *)) } ]
[ MAX COMBO <number> ]
[ WHERE <boolean_expression> ]
[ ORDER BY <column_name> ( ASC | DESC) }
[ LIMIT { <number> | ALL } ];
```
or using a `SPLIT` clause:
```sql
SELECT <column_name>,..., <column_name>
FROM DIFF (SPLIT [ <relation> | <subquery> ] WHERE <boolean_expression> )
ON { <column_name>,..., <column_name> | * }
[ WITH MIN SUPPORT <decimal> | MIN RATIO <decimal> ]
[ COMPARE BY { <ratio_metric_fn>(<aggregation_fn>(<column_name> | *)) } ]
[ MAX COMBO <number> ]
[ WHERE <boolean_expression> ]
[ ORDER BY <column_name> ( ASC | DESC) }
[ LIMIT { <number> | ALL } ];
```

### DIFF Output

In order for MacroBase to compatible with traditional SQL queries, the `DIFF`
operator has to be _composable_ -- it has to seamlessly integrate with other SQL
commands. (This is because SQL is fundamentally a [relational
algebra](https://en.wikipedia.org/wiki/Relational_algebra), except that it
operates on multi-sets instead of sets.)

This means that the `DIFF` operator has to output a relation, just like the
output of any other SQL operator: a `SELECT`, a `WHERE`, a `JOIN`, or a `GROUP BY`
always outputs a relation, so that users can layer additional queries or
clauses downstream. The `DIFF` operator does, in fact, output a relation---it
outputs the columns specified in the `ON` clause, plus three additional
columns: the ratio column, the **support** column, and the outlier counts
column.

For example, suppose we ran the following SQL query in MacroBase SQL:

```sql
SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON
    location, version
  COMPARE BY
    risk_ratio(COUNT(*)) ORDER BY support;
```

Then, the output of that query will look something like this:

```
-------------------------------------------------------------------------------------------
|    location     |     version     |   risk_ratio    |     support     |  outlier_count  |
-------------------------------------------------------------------------------------------
|       CAN       |       v2        |     1.5173      |     .210526     |       8.0       |
|      null       |       v1        |    10.456989    |     .789474     |      30.0       |
|       CAN       |       v1        |    46.424051    |     .789474     |      30.0       |
|       CAN       |      null       |        ∞        |       1.0       |      38.0       |
-------------------------------------------------------------------------------------------
```

Here, the `null` values correspond to an explanation that does not include a
value for that particular column; if you've written `GROUP BY` or `CUBE`
queries in SQL, these `nulll`s mean the exact same thing. (In database
parlance, the inclusion of these `null`s means we are returning a normalized
relation.) For example, the last row in the output above is examining the
explanation results for `location=CAN`; the row immediately above examines the
results for `location=CAN && version=v1`.

