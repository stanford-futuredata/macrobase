# MacroBase UI

This project was generated with [Angular CLI](https://github.com/angular/angular-cli) version 1.7.3.


##Installation and Setup

1. Install npm (javascript package manager) from https://www.npmjs.com/get-npm.

2. Navigate to "/ui" folder in macrobase.

3. In the command line, run "npm install" to install the requisite packages.

4. In the command line, run "ng serve" to start the UI running at http://localhost:4200/.

5. Navigate to the root folder in macrobase.

6. In the command line, run "bin/server.sh" to start the MacroBase server that the UI calls.

7. Open your browser and navigate to http://localhost:4200/ to run the UI.

In future, run only steps 4-7 to run the UI.

##Usage

The UI is set up with 4 tabs: Data, History, Edit, and Explore.

####Data
Use the Data tab to load a CSV file into MacroBase and specify which columns to use as metrics and which to use as explanatory attributes.

####History
Use the History tab to manage past queries and select queries for editing or explorating. Select queries by clicking on them.

####Edit
Use the Edit tab to create new queries and edit old queries. Click "Run Query" to generate a SQL command based off of your input and run the command through MacroBase SQL to process the query. You can specify the following aspects of the query:

*Metric* - The column to analyze.

*Attributes* - The columns to be used to explain anomalies in the metric.

*Percentile* - The percentile cutoff in the metric past which the rows should be treated as outliers to be explained.

*Support* - The minimum support for a given set of attributes to be considered a valid explanation. Support is the proportion of outlier rows that contain a given set of attributes.

*Risk Ratio* - The minimum risk ratio for a given set of attributes to be considered a valid explanation. Risk ratio is the proportion of rows corresponding to a given attribute set that are outliers divided by the proportion of rows not corresponding to that attribute set that are outliers. Intuitively, this represents how much more likely a data point is to be an outlier if it contains this attribute set.

####Explore
Use the Explore tab to compare explanations between queries and plot histograms of metrics filtered by explanations. Click on explanations to select them for plotting. Select queries to explore in the History tab.