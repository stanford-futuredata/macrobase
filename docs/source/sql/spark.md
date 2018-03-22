## Setup

Before going through this tutorial, we recommend going through the [SQL tutorial](tutorial)
to better understand MacroBase-SQL.  

To install MacroBase-Spark, download the latest
MacroBase-Spark release from our 
[GitHub page](https://github.com/stanford-futuredata/macrobase/releases) and
build from source.  Make sure you download a MacroBase-Spark release.

Building MacroBase-SQL-Spark requires [Apache Maven 3.3.9](https://maven.apache.org/)
and Java 8+.  Once Maven and Java 8 are installed, simply run `mvn package`
in the top-level directory, and MacroBase-SQL-Spark should successfully build.

In order to run MacroBase-SQL-Spark, your computer must be connected to an 
already-existing Spark cluster with HDFS set up.  Your `$SPARK_HOME`
environment variable must be set to your top-level Spark directory
and the `$SPARK_HOME/bin` directory must be on your system path.

## Running Spark Locally

This tutorial will use the same dataset as the [SQL tutorial](tutorial),
a sample of Wikipedia edits from
September 12, 2015. You can download the sample data here:
[wikiticker.csv](/wikiticker.csv). (Make sure to download the file to your
top-level MacroBase directory.)

First, we will discuss how to run MacroBase-SQL-Spark locally, 
then explain how to run it on a cluster.  To start up a local MacroBase-SQL-Spark
shell, run the following command from MacroBase's top-level directory:

```bash
spark-submit --master "local[2]" --class edu.stanford.futuredata.macrobase.sql.MacroBaseSQLRepl sql/target/macrobase-sql-1.0-SNAPSHOT.jar  -d -n 2
```

We will explain  how this command was constructed.  `spark-submit` is Spark's
built-in submission script, documented 
[here](https://spark.apache.org/docs/latest/submitting-applications.html).
The `--master` switch specifies the master of the Spark cluster to submit to; since we are
running locally, we tell Spark to use `local[2]`, or two cores of our local computer.  `--class`
tells Spark where to begin execution.  The jar is the application jar.  All arguments following
the jar are passed to the application directly.  In this case, the `-d` flag tells MacroBase-SQL
to distribute and use Spark.  The `-n` flag tells MacroBase-SQL-Spark how many partitions
to make when distributing computation.  In this case, since we have only two cores
to distribute over, we use two partitions.

Once MacroBase-SQL-Spark is running, it takes in the same commands as MacroBase-SQL.
Upon launching the shell, you should see:

```
Welcome to MacroBase!
macrobase-sql>
```

As in the previous tutorial, let's load in our CSV file:

```sql
IMPORT FROM CSV FILE 'wikiticker.csv' INTO wiki(time string, user string, page
  string, channel string, namespace string, comment string, metroCode string,
  cityName string, regionName string, regionIsoCode string, countryName string,
  countryIsoCode string, isAnonymous string, isMinor string, isNew string,
  isRobot string, isUnpatrolled string, delta double, added double, deleted
  double);
```

This command, like most others, behaves identically as it does in MacroBase-SQL, but distributes
its work across a Spark cluster.  Let's try a command that's unique to MacroBase-SQL,
like a `DIFF`:

```sql
SELECT * FROM
  DIFF
    (SELECT * FROM wiki WHERE deleted > 0.0) outliers,
    (SELECT * FROM wiki WHERE deleted <= 0.0) inliers
  ON channel, namespace, comment, metroCode, cityName, regionName, regionIsoCode,
  countryName, countryIsoCode, isAnonymous, isMinor, isNew, isRobot, isUnpatrolled;
```
  
Unlike MacroBase-SQL, MacroBase-SQL-Spark does not yet support `ON *` so we must
 specify all attributes to `DIFF` over.  


As in MacroBase-SQL, you can also write a `DIFF` query using our `SPLIT` operator. 
We can rewrite our initial `DIFF` query more concisely
and get the exact same result:

```sql
SELECT * FROM DIFF (SPLIT wiki WHERE deleted > 0.0)
  ON channel, namespace, comment, metroCode, cityName, regionName, regionIsoCode,
  countryName, countryIsoCode, isAnonymous, isMinor, isNew, isRobot, isUnpatrolled;
```

We can also still tweak parameters using `WITH MIN RATIO` and/or `MIN SUPPORT`:

```sql
SELECT * FROM DIFF (SPLIT wiki WHERE deleted > 0.0)
  ON channel, namespace, comment, metroCode, cityName, regionName, regionIsoCode,
  countryName, countryIsoCode, isAnonymous, isMinor, isNew, isRobot, isUnpatrolled
  WITH MIN SUPPORT 0.10;

SELECT * FROM DIFF (SPLIT wiki WHERE deleted > 0.0)
  ON channel, namespace, comment, metroCode, cityName, regionName, regionIsoCode,
  countryName, countryIsoCode, isAnonymous, isMinor, isNew, isRobot, isUnpatrolled
  WITH MIN RATIO 1.25;

SELECT * FROM DIFF (SPLIT wiki WHERE deleted > 0.0)
  ON channel, namespace, comment, metroCode, cityName, regionName, regionIsoCode,
  countryName, countryIsoCode, isAnonymous, isMinor, isNew, isRobot, isUnpatrolled
  WITH MIN SUPPORT 0.10 MIN RATIO 1.25;
  -- WITH MIN RATIO 1.25 MIN SUPPORT 0.10 also works
```

## Running Spark on a Cluster

Running MacroBase-SQL-Spark on a cluster is largely the same as running it locally.  The
major differences are in ingest and in the launch command.  Ingest on a cluster is done
through HDFS, so we must first put our file in our cluster's HDFS system.  Here's one 
way to do it:

```bash
hadoop fs -put FILE.csv /user/USERNAME
```

Afterwards, we launch MacroBase-SQL-Spark as before, but substituting the actual
master URL of the cluster for `local[2]` in the supplied command.  Depending
on the cluster, we may want to tune other properties, such as the number
of executors to use or the amount of memory to use on each executor.  Full 
documentation is [here](https://spark.apache.org/docs/latest/submitting-applications.html).
In addition to this, we may want to tune the number of partitions made by MacroBase
using the `-n` flag; at the minimum this should be set to the number of executing
cores in the cluster.

