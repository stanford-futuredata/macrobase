# MacroBase SQL

In addition to the UI and Java API, you can also write your MacroBase jobs as
SQL queries from the command line, using our custom shell. This enables
interactive exploration of your data using our MacroBase operators.

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
  all possible columns, use `ON *`.

- `COMPARE BY` specifies a **ratio metric function**, such as `risk_ratio`,
  `global_ratio`, or `prevalence_ratio`. Users must also specify an **aggregation
  function** (such as `COUNT`, or `AVG`) that takes a column (or multiple
  columns) as an argument.

- You can also specify an optional keyword called `MAX COMBO`, that specifies
  the maximum order you want for your generated explanations (`MAX COMBO [number]`).

- Remember: a `DIFF` query is just SQL! So you can include any other standard
  SQL clause: you can add `WHERE` clauses, `ORDER BY`s, and `LIMIT`s, for
  example. (`GROUP BY` and `HAVING` is not yet supported.) For example, if you
  want to prune out results with low support (e.g., 0.2), simply add `WHERE support > 0.2`
  to your SQL query. (By default, DIFF queries prune out all
  results with support less than 0.2, and ratios less than 1.5.)

#### Summary
Overall, the formal definition of a `DIFF` query looks something like this:
```sql
SELECT <column_name>,..., <column_name>
FROM DIFF (<relation>, <relation>)
ON { <column_name>,..., <column_name> | * }
COMPARE BY { <ratio_metric_fn>(<aggregation_fn>(<column_name> | *)) }
[ MAX COMBO <number> ]
[ WHERE <boolean_expression> ]
[ ORDER BY <column_name> ( ASC | DESC) }
[ LIMIT { <number> | ALL } ];
```

### DIFF Output

In order for MacroBase to compatible with traditional SQL queries, the `DIFF`
operator has to be _composable_---it has to seamlessly integrate with other SQL
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

For example, suppose we ran the following SQL query in Macrobase-SQL:

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


## Building MacroBase SQL

MacroBase-SQL depends on `macrobase-lib`, so build and install the `lib/`
directory if you haven't already: `cd lib && mvn install && cd -`. Then build
`macrobase-sql`: `cd sql && mvn package && cd -`.

## Running Macrobase SQL

To run MacroBase-SQL, run `bin/macrobase-sql`. You can also import a .sql file
with pre-written SQL queries; just run `bin/macrobase-sql -f [path/to/file]`.

You should see this:

```
Welcome to
    __  ___                      ____                
   /  |/  /___ _______________  / __ )____ _________ 
  / /|_/ / __ `/ ___/ ___/ __ \/ __  / __ `/ ___/ _ \
 / /  / / /_/ / /__/ /  / /_/ / /_/ / /_/ (__  )  __/
/_/  /_/\__,_/\___/_/   \____/_____/\__,_/____/\___/ 

macrobase-sql>
```

### Demo

If you run the `bin/macrobase-sql -f sql/demo.sql`, you should see the following output:

```
Welcome to
    __  ___                      ____
   /  |/  /___ _______________  / __ )____ _________
  / /|_/ / __ `/ ___/ ___/ __ \/ __  / __ `/ ___/ _ \
 / /  / / /_/ / /__/ /  / /_/ / /_/ / /_/ (__  )  __/
/_/  /_/\__,_/\___/_/   \____/_____/\__,_/____/\___/


IMPORT FROM CSV FILE 'core/demo/sample.csv' INTO sample(usage double, latency double, location string, version string);

1057 rows
-----------------------------------------------------
|   usage    |  latency   |  location  |  version   |
-----------------------------------------------------
|   30.77    |   238.0    |    CAN     |     v2     |
|   31.28    |   611.0    |    CAN     |     v2     |
|   31.17    |   768.0    |    RUS     |     v4     |
|   30.94    |   192.0    |    AUS     |     v3     |
|   35.36    |   401.0    |     UK     |     v3     |
|   39.12    |   531.0    |    RUS     |     v4     |
|    33.9    |   223.0    |     UK     |     v3     |
...
|  1000.77   |   864.0    |    CAN     |     v2     |
|  1000.77   |   864.0    |    CAN     |     v2     |
|  1000.77   |   864.0    |    CAN     |     v2     |
|  1000.77   |   864.0    |    CAN     |     v2     |
|  1000.77   |   864.0    |    CAN     |     v2     |
|  1000.77   |   864.0    |    CAN     |     v2     |
|  1000.77   |   864.0    |    CAN     |     v2     |
-----------------------------------------------------

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON
    location, version
  COMPARE BY
    risk_ratio(COUNT(*)) ORDER BY support;

4 rows
-------------------------------------------------------------------------------------------
|    location     |     version     |   risk_ratio    |     support     |  outlier_count  |
-------------------------------------------------------------------------------------------
|       CAN       |       v2        |     1.5173      |     .210526     |       8.0       |
|      null       |       v1        |    10.456989    |     .789474     |      30.0       |
|       CAN       |       v1        |    46.424051    |     .789474     |      30.0       |
|       CAN       |      null       |        ∞        |       1.0       |      38.0       |
-------------------------------------------------------------------------------------------

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON
    location, version
  COMPARE BY
    global_ratio(COUNT(*)) WHERE global_ratio > 10.0;

1 row
-------------------------------------------------------------------------------------------
|    location     |     version     |  global_ratio   |     support     |  outlier_count  |
-------------------------------------------------------------------------------------------
|       CAN       |       v1        |    10.562958    |     .789474     |      30.0       |
-------------------------------------------------------------------------------------------

```
