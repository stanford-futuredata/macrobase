package macrobase.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Lists;
import com.google.api.services.monitoring.v3.Monitoring;
import com.google.api.services.monitoring.v3.Monitoring.Projects;
import com.google.api.services.monitoring.v3.MonitoringScopes;
import com.google.api.services.monitoring.v3.model.ListTimeSeriesResponse;
import com.google.api.services.monitoring.v3.model.Point;
import com.google.api.services.monitoring.v3.model.TimeSeries;
import io.dropwizard.jackson.Jackson;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

/**
 * An ingester that fetches data from the Google Monitoring API.
 *
 * The ingester
 *
 * @see "https://cloud.google.com/monitoring/api/v3/"
 */
public class GoogleMonitoringIngester extends DataIngester {
    private static final Logger log = LoggerFactory.getLogger(GoogleMonitoringIngester.class);

    // Queries given as a JSON string. Example:
    // {
    //   "queries": [
    //     {
    //       "project": "my-project",
    //       "filter": "metric.type=\"custom.googleapis.com/test\"",
    //       "alignmentPeriod": "300s",
    //       "perSeriesAligner": "ALIGN_MEAN",
    //       "crossSeriesReducer": "REDUCE_NONE",
    //       "groupByFields": []
    //     }
    //   ]
    // }
    public static final String GOOGLE_MONITORING_QUERIES = "macrobase.loader.googlemonitoring.queries";
    // Start and end times given in RFC3339 format. Example: "2016-08-08T12:00:00.0000Z"
    public static final String GOOGLE_MONITORING_START_TIME = "macrobase.loader.googlemonitoring.startTime";
    public static final String GOOGLE_MONITORING_END_TIME = "macrobase.loader.googlemonitoring.endTime";

    private MBStream<Datum> dataStream;

    private boolean loaded = false;
    private int pointsAdded = 0;
    private int skippedTimeSeries = 0;
    private int skippedPoints = 0;

    public GoogleMonitoringIngester(MacroBaseConf conf) throws ConfigurationException, IOException {
        super(conf);
    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        if (!loaded) {
            QueryConf queryConf = getQueries(conf.getString(GOOGLE_MONITORING_QUERIES));
            String queryStart = conf.getString(GOOGLE_MONITORING_START_TIME);
            String queryEnd = conf.getString(GOOGLE_MONITORING_END_TIME);

            if (metrics.size() == 0) {
                throw new IllegalArgumentException("No metrics selected.");
            }

            // Record the attribute names.
            int idx = 1;
            Set<String> sortedAttributes = new TreeSet<>(attributes);
            for (String a : sortedAttributes) {
                conf.getEncoder().recordAttributeName(idx, a);
                ++idx;
            }

            // Each TimeSeries returned has a unique set of metric/resource labels and a stream
            // of values. Restructure the data to correlate by time so that we can ensure each Datum
            // contains the requested metrics and attributes. To ensure that the streams returned
            // by the API can be correlated by time, supply a per-series aligner.
            //
            // {timestamp, {attr_key, record}}
            Map<String, Map<String, Record>> byTime = new TreeMap<>();

            Monitoring client = buildClient();
            for (QueryConf.Query query : queryConf.getQueries()) {
                String pageToken = "";

                do {
                    Projects.TimeSeries.List request = client.projects().timeSeries()
                        .list("projects/" + query.getProject())
                        .setFilter(query.getFilter())
                        .setIntervalStartTime(queryStart)
                        .setIntervalEndTime(queryEnd)
                        .setPageToken(pageToken)
                        .setAggregationAlignmentPeriod(query.getAlignmentPeriod())
                        .setAggregationPerSeriesAligner(query.getPerSeriesAligner())
                        .setAggregationCrossSeriesReducer(query.getCrossSeriesReducer())
                        .setAggregationGroupByFields(query.getGroupByFields());
                    log.trace("Request: {}", request.toString());

                    ListTimeSeriesResponse response = request.execute();
                    log.trace("Response: {}", response.toPrettyString());

                    processResponse(response, metrics, byTime);
                    pageToken = response.getNextPageToken();
                } while (pageToken != null && !pageToken.isEmpty());
            }

            dataStream = convertToStream(byTime);

            log.info("Loaded {} points. Skipped {} TimeSeries and {} partial records.",
                     pointsAdded, skippedTimeSeries, skippedPoints);
            loaded = true;
        }

        return dataStream;
    }

    // Package scope to allow testing.
    QueryConf getQueries(String queryJson) throws IOException {
        ObjectMapper mapper = Jackson.newObjectMapper();
        return mapper.readValue(queryJson, QueryConf.class);
    }

    // Package scope to allow testing.
    void processResponse(ListTimeSeriesResponse response, List<String> allMetrics,
                         Map<String, Map<String, Record>> byTime) {

        for (TimeSeries ts : response.getTimeSeries()) {
            if (!allMetrics.contains(ts.getMetric().getType())) {
                ++skippedTimeSeries;
                continue;
            }

            // Extract attribute values from TimeSeries metric/resource labels. If some are
            // missing, skip this time series. Keep the keys sorted so that they correspond
            // to the attribute names recorded in the encoder.
            Map<String, String> attrMap = new TreeMap<>();
            boolean hasAllAttributes = true;

            for (String a : attributes) {
                String val = ts.getMetric().getLabels().get(a);
                if (val == null) {
                    val = ts.getResource().getLabels().get(a);
                    if (val == null) {
                        log.debug("Skipping TimeSeries due to missing attribute: " + a);
                        hasAllAttributes = false;
                        break;
                    }
                }
                attrMap.put(a, val);
            }

            if (!hasAllAttributes) {
                ++skippedTimeSeries;
                continue;
            }
            String attrKey = attrMap.toString();

            for (Point p : ts.getPoints()) {
                String timestamp = p.getInterval().getEndTime();

                byTime.putIfAbsent(timestamp, new HashMap<>());
                Record rec = byTime.get(timestamp).putIfAbsent(attrKey, new Record());
                if (rec == null) {
                    rec = byTime.get(timestamp).get(attrKey);
                    rec.attributes = attrMap;
                    rec.values = new HashMap<>();
                }

                double val;
                switch (ts.getValueType()) {
                    case "DOUBLE":
                        val = p.getValue().getDoubleValue();
                        break;
                    case "INT64":
                        val = p.getValue().getInt64Value();
                        break;
                    case "BOOL":
                        val = (p.getValue().getBoolValue()) ? 1.0 : 0.0;
                        break;
                    default:
                        continue;
                }

                rec.values.put(ts.getMetric().getType(), val);
            }
        }
    }

    // Package scope to allow testing.
    Datum processRecord(Record rec) throws Exception {
        int idx = 0;
        RealVector metricVec = new ArrayRealVector(metrics.size());
        for (String metric : metrics) {
            metricVec.setEntry(idx, rec.values.get(metric));
            ++idx;
        }

        idx = 1;
        List<Integer> attrList = new ArrayList<>(attributes.size());
        for (String attr : attributes) {
            attrList.add(conf.getEncoder().getIntegerEncoding(idx, rec.attributes.get(attr)));
            ++idx;
        }

        return new Datum(attrList, metricVec);
    }

    // Package scope to allow testing.
    MBStream<Datum> convertToStream(Map<String, Map<String, Record>> byTime) {
        MBStream<Datum> stream = new MBStream<>();
        for (Entry<String, Map<String, Record>> entry : byTime.entrySet()) {
            for (Record rec : entry.getValue().values()) {
                try {
                    stream.add(processRecord(rec));
                    ++pointsAdded;
                } catch (Exception e) {
                    // The record was incomplete.
                    ++skippedPoints;
                }
            }
        }

        return stream;
    }

    /**
     * Establishes an authenticated client using Application Default Credentials.
     *
     * @see "https://cloud.google.com/monitoring/demos/run_samples#before_you_begin"
     */
    private Monitoring buildClient() throws GeneralSecurityException, IOException {
        // Grab the Application Default Credentials from the environment.
        GoogleCredential credential = GoogleCredential.getApplicationDefault()
            .createScoped(MonitoringScopes.all());

        // Create and return the CloudMonitoring service object
        HttpTransport httpTransport = new NetHttpTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        return new Monitoring.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName("MacroBase Ingester")
            .build();
    }

    // Package scope to allow testing.
    static class Record {
        public Map<String, String> attributes;
        // {metric_type, value}
        public Map<String, Double> values;
    }

    // A POJO that that holds configuration information about the API queries to run. The
    // object is deserialized from JSON.
    // Package scope to allow testing.
    static class QueryConf {
        static class Query {
            private String project = "";
            private String filter = "";
            private String alignmentPeriod = "";
            private String perSeriesAligner = "ALIGN_NONE";
            private String crossSeriesReducer = "REDUCE_NONE";
            private List<String> groupByFields = Lists.newArrayList();

            public String getProject() {
                return project;
            }

            public void setProject(String project) {
                this.project = project;
            }

            public String getFilter() {
                return filter;
            }

            public void setFilter(String filter) {
                this.filter = filter;
            }

            public String getAlignmentPeriod() {
                return alignmentPeriod;
            }

            public void setAlignmentPeriod(String alignmentPeriod) {
                this.alignmentPeriod = alignmentPeriod;
            }

            public String getPerSeriesAligner() {
                return perSeriesAligner;
            }

            public void setPerSeriesAligner(String perSeriesAligner) {
                this.perSeriesAligner = perSeriesAligner;
            }

            public String getCrossSeriesReducer() {
                return crossSeriesReducer;
            }

            public void setCrossSeriesReducer(String crossSeriesReducer) {
                this.crossSeriesReducer = crossSeriesReducer;
            }

            public List<String> getGroupByFields() {
                return groupByFields;
            }

            public void setGroupByFields(List<String> groupByFields) {
                this.groupByFields = groupByFields;
            }
        }

        private List<Query> queries = Lists.newArrayList();

        public List<Query> getQueries() {
            return queries;
        }

        public void setQueries(List<Query> queries) {
            this.queries = queries;
        }
    }
}

