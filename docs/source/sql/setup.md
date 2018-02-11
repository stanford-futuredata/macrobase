## Installation Instructions

### Docker

The easiest way to install MacroBase SQL is by downloading our Docker image, which has everything properly configured for you out of the box. Simply run

```
docker pull macrobase/macrobase
```

to download the latest version. Then, you can run a new container by executing

```
docker run -i -t macrobase/macrobase /bin/bash
```

### Building from Source

You can also download the latest release of MacroBase SQL from our GitHub page and install it from source: <https://github.com/stanford-futuredata/macrobase/releases>.

Building MacroBase SQL requires [Apache Maven 3.3.9](https://maven.apache.org/)
and Java 8+.  Once Maven and Java 8 are installed, simply run `./build.sh sql`
in the top-level directory, and MacroBase SQL should successfully build.

## Running MacroBase SQL

Once you've installed MacroBase SQL, you can run it by executing `bin/macrobase-sql`.
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

You can also import a .sql file with pre-written SQL queries; just run `bin/macrobase-sql -f [path/to/file]`.

## Demo

To make sure MacroBase SQL has been successfully built, we have a simple demo
on sample data to get you started. If you run the `bin/macrobase-sql -f sql/demo.sql`, you should see the following output:

```
Welcome to
    __  ___                      ____                
   /  |/  /___ _______________  / __ )____ _________ 
  / /|_/ / __ `/ ___/ ___/ __ \/ __  / __ `/ ___/ _ \
 / /  / / /_/ / /__/ /  / /_/ / /_/ / /_/ (__  )  __/
/_/  /_/\__,_/\___/_/   \____/_____/\__,_/____/\___/ 
                                                     

IMPORT FROM CSV FILE 'core/demo/sample.csv' INTO sample(usage double, latency
  double, location string, version string);

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
  ON *;

2 rows
-------------------------------------------------------------------------------------------------------
|    location    |    version     |    support     |  global_ratio  |    outliers    |     count      |
-------------------------------------------------------------------------------------------------------
|      CAN       |       -        |      1.0       |    4.11284     |      38.0      |     257.0      |
|       -        |       v1       |    .789474     |    2.990945    |      30.0      |     279.0      |
-------------------------------------------------------------------------------------------------------

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON
    location, version;

2 rows
-------------------------------------------------------------------------------------------------------
|    location    |    version     |    support     |  global_ratio  |    outliers    |     count      |
-------------------------------------------------------------------------------------------------------
|      CAN       |       -        |      1.0       |    4.11284     |      38.0      |     257.0      |
|       -        |       v1       |    .789474     |    2.990945    |      30.0      |     279.0      |
-------------------------------------------------------------------------------------------------------

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON
    location, version
  WITH MIN RATIO 5.0 MIN SUPPORT 0.75
  COMPARE BY
    risk_ratio(COUNT(*));

2 rows
-------------------------------------------------------------------------------------------
|   location   |   version    |   support    |  risk_ratio  |   outliers   |    count     |
-------------------------------------------------------------------------------------------
|     CAN      |      -       |     1.0      |      ∞       |     38.0     |    257.0     |
|      -       |      v1      |   .789474    |  10.456989   |     30.0     |    279.0     |
-------------------------------------------------------------------------------------------

SELECT * FROM
  DIFF
    (SELECT * FROM sample WHERE usage > 1000.0) outliers,
    (SELECT * FROM sample WHERE usage < 1000.0) inliers
  ON *
  WITH MIN SUPPORT 0.75 MIN RATIO 5.0
  COMPARE BY
    risk_ratio(COUNT(*));

2 rows
-------------------------------------------------------------------------------------------
|   location   |   version    |   support    |  risk_ratio  |   outliers   |    count     |
-------------------------------------------------------------------------------------------
|     CAN      |      -       |     1.0      |      ∞       |     38.0     |    257.0     |
|      -       |      v1      |   .789474    |  10.456989   |     30.0     |    279.0     |
-------------------------------------------------------------------------------------------

SELECT * FROM
  DIFF
    (SPLIT (
        SELECT *, percentile(usage) as pct FROM sample)
     WHERE pct > 0.9641)
  ON *
  WITH MIN SUPPORT 0.75 MIN RATIO 5.0;

1 row
-------------------------------------------------------------------------------------------------------
|    location    |    version     |    support     |  global_ratio  |    outliers    |     count      |
-------------------------------------------------------------------------------------------------------
|      CAN       |       v1       |    .789474     |   10.562958    |      30.0      |      79.0      |
-------------------------------------------------------------------------------------------------------
```
