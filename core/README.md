## MacroBase core pipelines and tools

This module supports creating standard pipelines 
which can then be called from a JSON-based REST API
or command line interface scripts.
Configuration parameters are set in yaml files.

Pipelines consist of operators from macrobase-lib
hooked together with possible pre/post processing.

DefaultBatchPipeline is the default pipeline.

#### Building the package
From the project root directory:

```
cd lib
mvn clean; mvn install

cd ../core
mvn clean; mvn package
```

#### CLI Operation

From the macrobase root directory:

```
./bin/cli.sh core/demo/batch.json
```

will attempt to summarize the outliers in `core/demo/sample.csv`

If the data is in the cube form (grouped and aggregated by attribute values) then you will need
to specify which columns contain the counts, mean, and standard deviation aggregations.
To run a pipeline over a sample data cube in `core/demo/sampled_cubed.csv`, 
replace the last line from above with:

```
./bin/cli.sh core/demo/cube.json
```

#### Rest Server

To run a simple pipeline from the REST server
using a percentile classifier & itemset mining explanation:

From the project root directory:

```
./bin/server.sh &
sleep 5;
./core/demo/query.sh
```

#### Configuration

Logging settings are stored in `config/logback.xml`
