If you're just getting started, please check out the [Tutorial](https://github.com/stanford-futuredata/macrobase/wiki/Tutorial). For questions, please contact `macrobase@cs.stanford.edu`.

# Power GUI Usage

## Analyzing CSV files from the GUI

In addition to SQL, MacroBase's UI also supports limited interaction with CSV (Comma Separated Values) files via the GUI. The GUI currently supports running analysis queries over CSV files but does not support plotting or interaction as with SQL.

To analyze a CSV file, enter the file name in the 'Base query' field, starting with `csv://`, then press the 'Submit' button. For example, if we wanted to analyze a file called `my_file.csv` located in directory `lake/data/pb`, we would enter `csv://lake/data/pb/my_file.csv` into the 'Base query' field of the UI, then press 'Submit'. Select attributes and metrics just like in SQL before, then press 'Analyze'.

Although the buttons for plotting will appear, MacroBase will not perform plotting or explanation and will likely display an error if you attempt to plot a CSV file.

## A note on GUI Configuration

The GUI will pick up any configuration flags from `conf/macrobase.yaml`. However, the UI supports a subset of all MacroBase features.

## Accessing additional SQL databases from the UI

You can connect to additional SQL databases by following the [directions below](#connecting-to-other-data-sources), placing the additional configuration parameters in the `conf/macrobase.yaml` file.

# Command line and Configuration Details

## Running from the command line

Many experimental MacroBase features must be run from the command line. To run from the command line, from the root of the macrobase source tree, run the command 'bin/batch.sh conf/batch.yaml'.

The file `conf/batch.yaml` (which you can replace with another) describes the actual query; everything that is input in the GUI can be input in this file via a text editor or programmatically via a script.

You can override the configuration file by passing in additional command line arguments to Java; for example, `-Dmacrobase.loader.db.url=localhost:9999`. The MacroBase scripts check for these arguments automatically in the `JAVA_OPTS` environment variable.

## Common configuration flags

You can find a guide to common configuration options in the `docs/` directory in the repo: [link](https://github.com/stanford-futuredata/macrobase/blob/master/docs/parameters_desc.md)

## Connecting to other data sources

*PostgreSQL and MySQL:* MacroBase has built-in support for analyzing PostgreSQL and MySQL databases. By default, MacroBase's UI attempts to connect to a PostgreSQL database. To instead connect to a MySQL database, add the following line to your configuration file (for the server, `conf/macrobase.yaml`):

    macrobase.loader.loaderType: MYSQL_LOADER

You can set additional JDBC properties via the `macrobase.loader.jdbc.properties` flag.

For username/password:

    macrobase.loader.db.user: USERNAME
    macrobase.loader.db.password: PASSWORD


*Custom ingesters:* You can also write your own ingester, or use an unofficial one from the `contrib` package. For example, Google has contributed an ingester for their [Stackdriver Monitoring API](https://cloud.google.com/monitoring/api/v3/). You can use this ingester by specifying the fully-qualified class name of the loader:

    macrobase.loader.loaderType: macrobase.ingest.GoogleMonitoringIngester

These loaders are generally not supported in the UI.

*JDBC connectors:* You can also connect to other databases such as Oracle and SAP HANA. Because several companies do not release their client connectors ("JDBC jars") via common software repositories that MacroBase can download for you, you'll have to download the client library yourself. For example, Oracle has libraries available [here](http://www.oracle.com/technetwork/database/features/jdbc/index-091264.html).

Once you download the corresponding jar file, you need to add it to your class path. On UNIX, you can run `export CLASSPATH=$CLASSPATH:/path/to/directory/with/your/jar`, or you can manually edit files like `bin/server.sh`. Subsequently, add the following parameters to your configuration file:

    macrobase.loader.loaderType: macrobase.ingest.CustomJDBCIngester
    macrobase.loader.jdbc.driver: <<JDBC driver name here>> oracle.jdbc.driver.OracleDriver
    macrobase.loader.jdbc.urlprefix: <<JDBC URL prefix here>>

For example, for Oracle:

    macrobase.loader.loaderType: macrobase.ingest.CustomJDBCIngester
    macrobase.loader.jdbc.driver: oracle.jdbc.driver.OracleDriver
    macrobase.loader.jdbc.urlprefix: jdbc:oracle:

## Running the CSV loader from the command line

To use the CSV loader, we need to tell MacroBase to use the CSV loader instead of the default SQL loader and we also need to tell it which file to read. The two key lines to accomplish this are:

    macrobase.loader.loaderType: CSV_LOADER
    macrobase.loader.csv.file: /path/to/my.csv

We put these lines in the configuration file as above. You can see an example at:
https://gist.github.com/pbailis/d5fff8d5662ea70e7a053f67da8b44be

MacroBase expects each CSV to begin with a line containing the column titles (called a "CSV header"), followed by one value per line. For example:

    device_name, reading1, reading2, reading3
    d1, 1, 2, 3
    d2, 2, 2, 3
    d3, 2, 2, 3

MacroBase uses the CSV header to know what entries correspond to the metrics and attributes you select in the configuration file.
