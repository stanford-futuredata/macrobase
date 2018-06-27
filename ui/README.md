# MacroBase UI

## Installation and Setup

Before setting up MacroBase UI, make sure you have built MacroBase (https://macrobase.stanford.edu/docs/sql/setup/).

1. Install npm (javascript package manager) from https://www.npmjs.com/get-npm.

2. Navigate to "/ui" folder in macrobase.

3. In the command line, run "npm install" to install the requisite packages.

4. In the command line, run "ng serve" to start the UI running at http://localhost:4200/.

5. Navigate to the root folder in macrobase.

6. In the command line, run "bin/server.sh" to start the MacroBase server that the UI calls.

7. Open your browser and navigate to http://localhost:4200/ to run the UI.

In future, run only steps 4-7 to run the UI.

## Query parameters

*Metric* - The column to analyze.

*Attributes* - The columns to be used to explain anomalies in the metric.

*Query Type* - Currently the UI only supports queries which classify outlying rows according to a percentile cutoff of the metric.

*Percentile* - The percentile cutoff in the metric past which the rows should be treated as outliers to be explained.

*Support* - The minimum support for a given set of attributes to be considered a valid explanation. Support is the proportion of outlier rows that contain a given set of attributes.

*Risk Ratio* - The minimum risk ratio for a given set of attributes to be considered a valid explanation. Risk ratio is the proportion of rows corresponding to a given attribute set that are outliers divided by the proportion of rows not corresponding to that attribute set that are outliers. Intuitively, this represents how much more likely a data point is to be an outlier if it contains this attribute set.