## MacroDiff

MacroDiff is MacroBase-on-SQL: you can write your MacroBase jobs as SQL queries
from the command line, using our custom shell. This enables interactive
exploration of your data using our MacroBase operators.

## New SQL Operators

To MacroBase queries in SQL, we've extend the standard SQL syntax to include two new operators: `DIFF` and `COMPARE BY`.

- `DIFF` takes in two relations as arguments: the first relation is the **outliers** relation, while the second is the **inliers** relation. This represents the classification stage of the MacroBase pipeline.
  - The `DIFF` operator also requires you to specify which attributes you want to consider for explanations. This is done using the `ON` keyword in SQL.
- `COMPARE BY` specifies a **ratio metric function**, such as `risk_ratio`, `pmi`, or `prevalence_ratio`. Users must also specify an **aggregation function** (such as `COUNT`, or `AVG`) that takes a column (or multiple columns) as an argument.
- You can also specify an optional keyword called `MAX COMBO`, that specifies the maximum order you want for the generated explanations, e.g. `MAX COMBO [number]`.

Overall, our syntax looks like this end-to-end:
```sql
SELECT * FROM
	DIFF 
	{ ( (<relation>, <relation>) | <relation> ) }
ON
{ <column_name>,…,<column_name> | * }
COMPARE BY
{ <ratio_metric>(<aggregation_fn>(<column_name> | *)) [ AS <alias> ] }
[ MAX COMBO <number> ]
[ WHERE { <handle> | SUPPORT } <predicate> ]
[ ORDER BY <handle> ( ASC | DESC) }
[ LIMIT <number> ];
```
### Building MacroDiff

MacroDiff depends on `macrobase-lib`, so build and install the `lib/` directory if you haven't already: `cd lib && mvn install && cd -`. Then build `macrobase-repl`: `cd repl && mvn package && cd -`.

### Launching MacroDiff

To run MacroDiff, run `bin/macrodiff.sh`. You can also import a .sql file with pre-written SQL queries; just run `bin/macrodiff.sh -f [path/to/file]`.

You should see this:

```
Welcome to
    __  ___                      ____  _ ________
   /  |/  /___ _______________  / __ \(_) __/ __/
  / /|_/ / __ `/ ___/ ___/ __ \/ / / / / /_/ /_
 / /  / / /_/ / /__/ /  / /_/ / /_/ / / __/ __/
/_/  /_/\__,_/\___/_/   \____/_____/_/_/ /_/

macrodiff>
```

