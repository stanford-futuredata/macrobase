# MacroBase


## What is MacroBase?

MacroBase is a new *analytic monitoring* engine designed to prioritize human
attention in large-scale datasets and data streams. Unlike a traditional
analytics engine, MacroBase is specialized for one task: finding and explaining
unusual or interesting trends in data.

Under the hood, MacroBase is architected for rapid adaptation to new
application domains. MacroBase has been successfully used for analyses in
domains including datacenter and mobile application monitoring, industrial
manufacturing, and video and satellite image analysis.

MacroBase supports streaming operation, meaning that once users identify an
important behavior, they can track it continuously over time.

## How can I run MacroBase queries?

The simplest way to use MacroBase is through our [SQL interface](sql/docs).
Using our extended version of SQL, users can segment their datasets by key
performance metrics (e.g., power drain, latency) and query for explanations of
anomalous or abnormal behavior across important metadata attributes (e.g.,
hostname, device ID). For example, MacroBase may report that queries running on
host #5 are 10x more likely to experience high latency than the rest of the
cluster, or that devices of type "X103" are 30x more likely to experience high
power drain.

MacroBase also has a UI that offers simple point-and-click functionality
for specifying user queries. Users can highlight the key performance metrics
they care about, select which metadata attributes to explore, and let MacroBase
find explanations of the anomalous or interesting trends in the data. For more,
check out our [UI Tutorial](gui/tutorial).

MacroBase is designed for extensibility, and there are a number of more
advanced interfaces you can use to program the system and execute more complex
query functionality.

## More information

Feel free to open GitHub issues or email us at macrobase@cs.stanford.edu.  The
development branches contain most of the action.  A detailed technical overview
of the project is available [on arXiv](http://arxiv.org/pdf/1603.00567.pdf).
