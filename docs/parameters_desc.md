# A description of the parameters in play with Macrobase

This document describes the important parameters in Macrobase that
affect its behavior.

## Workload-specific parameters

We first describe workload-specific parameters. Macrobase will throw an exception if 
parameters that don't have a default value aren't specified.

<table class="table">
<tr><th>Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>macrobase.query.name</code></td>
  <td>(none)</td>
  <td>
  Human-readable task name.
  </td>
</tr>
<tr>
  <td><code>macrobase.loader.attributes</code></td>
  <td>(none)</td>
  <td>
  List of attributes to use in summarization. These
must be categorical.
  </td>
</tr>
<tr>
  <td><code>macrobase.loader.targetHighMetrics</code></td>
  <td>(none)</td>
  <td>
  List of metrics to detect outliers from (along with <code>targetLowMetrics</code>. These
metrics have <i>high</i> values, and can be easily separated as is. Must be a floating
point number.
  </td>
</tr>
<tr>
  <td><code>macrobase.loader.targetLowMetrics</code></td>
  <td>(none)</td>
  <td>
  List of metrics to detect outliers.
By specifying a metric as <i>low</i>, Macrobase searches for low values by taking the
reciprocal of the value for each point (i.e. 1/value).
Must be a real number.
  </td>
</tr>
<tr>
  <td><code>macrobase.loader.baseQuery</code></td>
  <td>(none)</td>
  <td>
  Query used to specify input data source. Can be used to restrict
the rows or columns Macrobase needs to look at. For example,
<code>SELECT tabl1.col1, tabl1.col2, tabl2.col3 FROM tabl1, tabl2 WHERE tabl1.id == tabl2.id;</code>
  </td>
</tr>
<tr>
  <td><code>macrobase.loader.db.user</code></td>
  <td><code>System.getProperty("user.name")</code></td>
  <td>
  Username to access input database.
  </td>
</tr><tr>
  <td><code>macrobase.loader.db.password</code></td>
  <td><code>""</code></td>
  <td>
  Password to access input database.
  </td>
</tr>
<tr>
  <td><code>macrobase.loader.db.database</code></td>
  <td><code>postgres</code></td>
  <td>
  Name of database.
  </td>
</tr><tr>
  <td><code>macrobase.loader.db.url</code></td>
  <td><code>localhost</code></td>
  <td>
  URL of database.
  </td>
</tr>
<tr>
  <td><code>macrobase.loader.db.cacheDirectory</code></td>
  <td>(none)</td>
  <td>
  Directory location of local cache.
  </td>
</tr>
</table>

## Analysis-specific parameters

The following are parameters relavant to both streaming and batch
workloads.

<table class="table">
<tr><th>Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>macrobase.analysis.transformType</code></td>
  <td><code>MCD_OR_MAD</code></td>
  <td>
  Type of outlier detection algorithm to use.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.minSupport</code></td>
  <td><code>0.001</code></td>
  <td>
  Used in summarization: threshold for determining whether
combinations of attributes have sufficient support (i.e., appear
in more than a given proportion of points) within the outlier points.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.minOIRatio</code></td>
  <td><code>3.0</code></td>
  <td>
  Only attribute combinations that occur
in the outliers <code>minInlierRatio</code> more times than in the inliers are returned.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.targetPercentile</code> and <code>macrobase.analysis.usePercentile</code></td>
  <td><code>0.99</code> and <code>true</code></td>
  <td>
  If <code>true></code>, determine outliers according to a fixed percentile of scores.
Points with scores in the <code>targetPercentile</code> or greater will be marked as outliers.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.zScore</code> and <code>macrobase.analysis.useZScore</code></td>
  <td><code>3.0</code> and <code>false</code></td>
  <td>
  If <code>true</code>, determine outliers according to the detector's
Z-Score equivalent. Points with scores greater than <code>macrobase.analysis.zScore</code>
will be marked as outliers.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.randomSeed</code></td>
  <td><code>null</code></td>
  <td>
  Controls the seed of the random number generator.
  </td>
</tr>
</table>

## Outlier detection-specific parameters

The following are parameters specific to particular outlier detection algorithms.

<table class="table">
<tr><th>Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>macrobase.analysis.mcd.alpha</code></td>
  <td><code>0.5</code></td>
  <td>
  This is a <b>MCD-only</b> parameter. Controls the size of
the cloud of points sampled from the entire dataset of points. In the first
example described
<a href="https://tr8dr.wordpress.com/2010/09/24/minimum-covariance-determination/">here</a>
, <code>alphaMCD = 0.9</code>, for example.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.mcd.stoppingDelta</code></td>
  <td><code>0.001</code></td>
  <td>
  This is again a <b>MCD-only</b> parameter. Controls
the stopping condition in the training phase of MCD in the C-step of FastMCD.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.kde.bandwidth</code></td>
  <td><code>1.0</code></td>
  <td>
  This is a <b>KDE-only</b> parameter. Controls the width
of the kernel in the KDE outlier deteciton algorithm.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.kde.kernelType</code></td>
  <td><code>EPANECHNIKOV_MULTIPLICATIVE</code></td>
  <td>
  This is a <b>KDE-only</b> parameter. Controls the type of kernel
used in the KDE outlier detection algorithm.
  </td>
</tr>
</table>

## Streaming-specific parameters

The following are streaming-specific parameters, and _only_ apply to
streaming workloads.

<table class="table">
<tr><th>Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>macrobase.analysis.streaming.summaryUpdatePeriod</code></td>
  <td><code>100,000</code></td>
  <td>
  Controls how frequently to decay the counts and re-structure the tree.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.streaming.modelUpdatePeriod</code></td>
  <td><code>100,000</code></td>
  <td>
  Controls how frequently the model needs to be refreshed.
Could be a time (in case <code>useRealTimePeriod</code> is set to <code>true</code>) or a tuple count
(in case <code>useTupleCountPeriod</code> is set to <code>true</code>).
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.streaming.useRealTimePeriod</code></td>
  <td><code>false</code></td>
  <td>
  If true, the model is re-trained periodically according to wall-clock time.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.streaming.useTupleCountPeriod</code></td>
  <td><code>true</code></td>
  <td>
  If true, the model is re-trained periodically according to the number of tuples
processed.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.streaming.decayRate</code></td>
  <td><code>0.01</code></td>
  <td>
  Exponential decay rate for input and score samples and summary attribute counts.
Decay is performed every period, configurable above.
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.streaming.inputReservoirSize</code></td>
  <td><code>10,000</code></td>
  <td>
  Controls the number of points sampled from the
input data stream. Whenever the outlier detection algorithm is trained, the tuples
that are currently in the input reservoir are used. The larger this parameter,
the more context the detection algorithms will have in the training phase (and
consequently, the longer the training phase will take).
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.streaming.scoreReservoirSize</code></td>
  <td><code>10,000</code></td>
  <td>
  Controls the number of points sampled from the
scored data stream. Used to estimate the threhold score when `usePercentile`
(described above) is set to <code>true</code>. The larger this parameter, the more accurate
the chosen threshold will be (although more time would be spent to keep
the reservoir sorted).
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.streaming.warmupCount</code></td>
  <td><code>10,000</code></td>
  <td>
  Number of tuples used to initialized the model.
  </td>
</tr>
</table>


## Contextual Outlier Detection-specific parameters

The following are contextual outlier detection-specific parameters.

<table class="table">
<tr><th>Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>macrobase.analysis.contextual.enabled</code></td>
  <td><code>false</code></td>
  <td>
  Controls whether to run contextual outlier detection or not
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.contextual.discreteAttributes</code></td>
  <td><code>none</code></td>
  <td>
  The set of discrete contextual attributes. For every discrete attribute A, we generate contexts A = a, for every active domain value
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.contextual.doubleAttributes</code></td>
  <td><code>none</code></td>
  <td>
  The set of double contextual attributes. For every double attribute A, we generate contexts A in [l,h) by discretizing the active domain into a list of intervals 
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.contextual.denseContextTau</code></td>
  <td><code>0.5</code></td>
  <td>
  The minimum percentage of tuples required for a context to be considered
  </td>
</tr>
<tr>
  <td><code>macrobase.analysis.contextual.numIntervals</code></td>
  <td><code>10</code></td>
  <td>
  The number of intervals for a double contextual attribute to be discretized into. Right now, it is equal width, i.e., every interval covers (max - min) / numInterval
  </td>
</tr>
</table>
