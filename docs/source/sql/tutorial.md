## Setup

Before going through this tutorial, follow the [setup
instructions](setup) to make sure MacroBase is correctly installed on
your machine.

# Analyzing Wikipedia Edits with MacroBase SQL

In this tutorial, we're going to analyze a sample of Wikipedia edits from
September 12, 2015. You can download the sample data here:
[wikiticker.csv](/wikiticker.csv). (Make sure to download the file to your
top-level MacroBase directory.)

Using MacroBase parlance, we can classify the schema of this dataset into
**metrics** (measurements in the dataset that capture user's interests) and
**attributes** (dimensions that could possibly explain why a metric is behaving
unusually).

Besides "time", here are the attributes present in the data:

- user
- page
- channel
- namespace
- comment
- metroCode
- cityName
- regionName
- regionIsoCode
- countryName
- countryIsoCode
- isAnonymous
- isMinor
- isNew
- isRobot
- isUnpatrolled

And here are the three metrics that are present:

* added   (number of lines added in the edit)
* deleted (number of lines deleted)
* delta   (number of lines that changed)

### Getting Started

From the top-level directory, run `bin/macrobase-sql` to start MacroBase
SQL shell -- you should see this:

```
Welcome to
    __  ___                      ____                
   /  |/  /___ _______________  / __ )____ _________ 
  / /|_/ / __ `/ ___/ ___/ __ \/ __  / __ `/ ___/ _ \
 / /  / / /_/ / /__/ /  / /_/ / /_/ / /_/ (__  )  __/
/_/  /_/\__,_/\___/_/   \____/_____/\__,_/____/\___/ 

macrobase-sql>
```

Next, let's load the CSV file into MacroBase SQL:

```sql
IMPORT FROM CSV FILE 'wikiticker.csv' INTO wiki(time string, user string, page
  string, channel string, namespace string, comment string, metroCode string,
  cityName string, regionName string, regionIsoCode string, countryName string,
  countryIsoCode string, isAnonymous string, isMinor string, isNew string,
  isRobot string, isUnpatrolled string, delta double, added double, deleted
  double);
```

MacroBase SQL is just SQL: you can add projections in the `SELECT` clause,
predicates in the `WHERE` clause, an `ORDER BY` clause (on single columns only -- for now),
and a `LIMIT` clause.

```sql
SELECT comment, channel FROM wiki
  WHERE countryIsoCode is not NULL
  ORDER BY channel DESC LIMIT 15;
```

To save the output of any query in MacroBase SQL to a file, use the `INTO
OUTFILE` syntax found in MySQL

```sql
SELECT comment, channel FROM wiki
  WHERE countryIsoCode is not NULL
  ORDER BY channel DESC
  INTO OUTFILE 'comments.csv' FIELDS TERMINATED BY '\t';
  -- FIELDS TERMINATED BY clause is optional; default is ','
```

Now, let's try out the features that are unique to MacroBase SQL, such as the `DIFF` operator.
Here's a simple `DIFF` query with two arguments: outliers and inliers.

```sql
SELECT * FROM
  DIFF
    (SELECT * FROM wiki WHERE deleted > 0.0) outliers,
    (SELECT * FROM wiki WHERE deleted <= 0.0) inliers
  ON *;
```
  
Notice the `ON *` at the end: MacroBase will automatically try to find
attribute columns that are good candidates for explanation. As the query is
run, you'll see which columns are selected. For example, in the query above
you should see something like this in the log output:

```
0    [main] INFO  QueryEngine  - Using channel, cityName, countryIsoCode, countryName, isAnonymous, isMinor, isNew, isRobot, isUnpatrolled, metroCode, namespace, regionIsoCode, regionName as candidate attributes for explanation
```

In this case, the time, user, and page columns were ignored, since they have
a unique value per row.

You should also see additional metadata about the query in the log output, 
like the minimum support (default: 0.2), minimum ratio (default: 1.5), and
ratio metric (default: global_ratio) used in the query:

```
4    [main] INFO  APriori  - Min Support Ratio: 0.2
5    [main] INFO  APriori  - Min Ratio Metric: 1.5
5    [main] INFO  APriori  - Using Ratio of: GlobalRatioMetric
```

You can also write a `DIFF` query using our `SPLIT` operator. The `SPLIT` operator
takes in a relation and a `WHERE` clause, and it effectively splits the input
relation into two output relations: one in which the `WHERE` clause always
evaluates to "true", and the other in which the `WHERE` clause always
evaluates to "false". We can rewrite our initial `DIFF` query much more concisely
and get the exact same result:

```sql
SELECT * FROM DIFF (SPLIT wiki WHERE deleted > 0.0) ON *;
```

Instead of a table name, you can also pass a subquery as the input relation
to a `SPLIT` operator. This again yields the exact same result:

```sql
SELECT * FROM DIFF (SPLIT (SELECT * FROM wiki) WHERE deleted > 0.0) ON *;
```

Note: A `SPLIT` clause can't be the top-level query in MacroBase SQL; the query below, for example, will result in a parsing error:

```sql
SPLIT wiki WHERE deleted > 0.0;
```

Many of the columns in `ON *` didn't yield any explanations (e.g.,
"countryName", "regionName"); we can filter these out by modifying the ON
clause to include only the columns we care about (which will also improve
query performance).

```sql
SELECT * FROM DIFF (SPLIT wiki WHERE deleted > 0.0)
  ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, namespace;
```

Maybe our original query (with `ON *`) didn't yield explanations in
"countryName" and "regionName" because our minimum support or minimum ratio were
too high; we can tweak either using `WITH MIN RATIO` and/or `MIN SUPPORT`:

```sql
SELECT * FROM DIFF (SPLIT wiki WHERE deleted > 0.0)
  ON *
  WITH MIN SUPPORT 0.10;

SELECT * FROM DIFF (SPLIT wiki WHERE deleted > 0.0)
  ON *
  WITH MIN RATIO 1.25;

SELECT * FROM DIFF (SPLIT wiki WHERE deleted > 0.0)
  ON *
  WITH MIN SUPPORT 0.10 MIN RATIO 1.25;
  -- WITH MIN RATIO 1.25 MIN SUPPORT 0.10 also works
```

We also have support for user-defined functions (UDFs) that can be applied to
individual columns in MacroBase SQL; these come in handy for more complicated
`SPLIT` clauses. For example, we can apply a percentile UDF to the "deleted"
column:

```sql
SELECT percentile(deleted) FROM wiki;
SELECT deleted, percentile(deleted) as percentile FROM wiki;
SELECT *, percentile(deleted) as percentile FROM wiki;
```

We can also execute UDFs in the `WHERE` clause to apply custom predicates to our data:

```sql
SELECT deleted FROM wiki WHERE percentile(deleted) > 0.95;
```

In the `SPLIT` clause, you can treat the UDF column as any other column:
```sql
SELECT * FROM DIFF
    (SPLIT (
      SELECT *, percentile(deleted) as percentile FROM wiki)
    WHERE percentile > 0.95)
  ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, namespace
  WITH MIN SUPPORT 0.10;
```

### Your Turn

There's plenty more to explore in this dataset! For example, try writing a
few queries using the `DIFF` and `SPLIT` operators to analyze the "added"
and "delta" metrics, which we haven't done yet. Are there any interesting
outliers that you can find?

Here's a sample query to get you started:

```sql
SELECT * FROM DIFF
  (SPLIT (
    SELECT *, percentile(delta) as percentile FROM wiki
    )
  WHERE percentile > 0.99)
ON *;
```

