package macrobase.ingest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.api.services.monitoring.v3.model.ListTimeSeriesResponse;
import com.google.api.services.monitoring.v3.model.Metric;
import com.google.api.services.monitoring.v3.model.MonitoredResource;
import com.google.api.services.monitoring.v3.model.Point;
import com.google.api.services.monitoring.v3.model.TimeInterval;
import com.google.api.services.monitoring.v3.model.TimeSeries;
import com.google.api.services.monitoring.v3.model.TypedValue;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.GoogleMonitoringIngester.QueryConf;
import macrobase.ingest.GoogleMonitoringIngester.Record;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GoogleMonitoringIngesterTest {
    private static final Logger log = LoggerFactory.getLogger(GoogleMonitoringIngesterTest.class);

    private MacroBaseConf conf;

    @Before
    public void setupConf() {
        conf = new MacroBaseConf();

        String queryJson =
            "{`queries`: [{`project`: `some-project`," +
            "`filter`: `metric.type=\\`custom.googleapis.com/test\\``," +
            "`alignmentPeriod`: `300s`," +
            "`perSeriesAligner`: `ALIGN_MEAN`," +
            "`crossSeriesReducer`: `REDUCE_MEAN`," +
            "`groupByFields`: [`foo`, `project`]}]}";
        conf.set(GoogleMonitoringIngester.GOOGLE_MONITORING_QUERIES, queryJson.replace('`', '"'));
        conf.set(GoogleMonitoringIngester.GOOGLE_MONITORING_START_TIME, "2016-08-08T00:00:00Z");
        conf.set(GoogleMonitoringIngester.GOOGLE_MONITORING_END_TIME, "2016-08-09T00:00:00Z");

        conf.set(MacroBaseConf.ATTRIBUTES, Lists.newArrayList("attr1", "attr2"));
        conf.set(MacroBaseConf.METRICS, Lists.newArrayList("custom.googleapis.com/test"));
    }

    @Test
    public void testGetQueries() throws Exception {
        GoogleMonitoringIngester ingester = new GoogleMonitoringIngester(conf);
        QueryConf queryConf = ingester.getQueries(conf.getString(GoogleMonitoringIngester.GOOGLE_MONITORING_QUERIES));

        assertEquals(1, queryConf.getQueries().size());

        QueryConf.Query query = queryConf.getQueries().get(0);
        assertEquals("some-project", query.getProject());
        assertEquals("metric.type=\"custom.googleapis.com/test\"", query.getFilter());
        assertEquals("300s", query.getAlignmentPeriod());
        assertEquals("ALIGN_MEAN", query.getPerSeriesAligner());
        assertEquals("REDUCE_MEAN", query.getCrossSeriesReducer());
        assertArrayEquals(Lists.newArrayList("foo", "project").toArray(),
                          query.getGroupByFields().toArray());
    }

    @Test
    public void testGetQueriesUsesDefaults() throws Exception {
        GoogleMonitoringIngester ingester = new GoogleMonitoringIngester(conf);
        QueryConf queryConf = ingester.getQueries(
            "{`queries`: [{`project`: `some-project`, `filter`: `metric.type=\\`custom.googleapis.com/test\\``}]}".replace('`', '"'));

        assertEquals(1, queryConf.getQueries().size());

        QueryConf.Query query = queryConf.getQueries().get(0);
        assertEquals("some-project", query.getProject());
        assertEquals("metric.type=\"custom.googleapis.com/test\"", query.getFilter());
        assertEquals("", query.getAlignmentPeriod());
        assertEquals("ALIGN_NONE", query.getPerSeriesAligner());
        assertEquals("REDUCE_NONE", query.getCrossSeriesReducer());
        assertEquals(0, query.getGroupByFields().size());
    }

    @Test
    public void testProcessRecord() throws Exception {
        Record record = new Record();
        record.attributes = new TreeMap<>();
        record.attributes.put("attr1", "foo");
        record.attributes.put("attr2", "bar");
        record.values = new HashMap<>();
        record.values.put("custom.googleapis.com/test", 77.0);

        GoogleMonitoringIngester ingester = new GoogleMonitoringIngester(conf);
        Datum datum = ingester.processRecord(record);

        assertEquals(77, datum.metrics().getEntry(0), 0.0);
        assertEquals(0, datum.attributes().get(0), 0.0);
        assertEquals(1, datum.attributes().get(1), 0.0);
    }

    @Test(expected = NullPointerException.class)
    public void testProcessRecordMissingMetric() throws Exception {
        Record record = new Record();
        record.attributes = new TreeMap<>();
        record.attributes.put("attr1", "foo");
        record.attributes.put("attr2", "bar");
        record.values = new HashMap<>();

        GoogleMonitoringIngester ingester = new GoogleMonitoringIngester(conf);
        ingester.processRecord(record);
    }

    @Test
    public void testProcessResponse() throws Exception {
        Map<String, String> metricLabels1 = new HashMap<>();
        metricLabels1.put("attr1", "foo");
        metricLabels1.put("attr2", "bar");

        Map<String, String> metricLabels2 = new HashMap<>();
        metricLabels2.put("attr1", "foo");
        metricLabels2.put("attr2", "baz");

        // Missing one of the attributes. Will be skipped.
        Map<String, String> metricLabels3 = new HashMap<>();
        metricLabels3.put("attr1", "foo");

        Metric metric1 = new Metric();
        metric1.setType("custom.googleapis.com/test");
        metric1.setLabels(metricLabels1);

        Metric metric2 = new Metric();
        metric2.setType("custom.googleapis.com/skipped");
        metric2.setLabels(metricLabels1);

        Metric metric3 = new Metric();
        metric3.setType("custom.googleapis.com/test");
        metric3.setLabels(metricLabels2);

        Metric metric4 = new Metric();
        metric4.setType("custom.googleapis.com/test");
        metric4.setLabels(metricLabels3);

        Map<String, String> resourceLabels1 = new HashMap<>();
        resourceLabels1.put("project_id", "my-project");
        resourceLabels1.put("region", "us-central-1");

        // Get the second attribute from the resource labels.
        Map<String, String> resourceLabels2 = new HashMap<>();
        resourceLabels2.put("project_id", "my-project");
        resourceLabels2.put("region", "us-central-1");
        resourceLabels2.put("attr2", "bar");

        MonitoredResource resource1 = new MonitoredResource();
        resource1.setType("gce_instance");
        resource1.setLabels(resourceLabels1);

        MonitoredResource resource2 = new MonitoredResource();
        resource2.setType("gce_instance");
        resource2.setLabels(resourceLabels2);

        TimeInterval time1 = new TimeInterval();
        time1.setEndTime("2016-08-08T00:00:00Z");

        TimeInterval time2 = new TimeInterval();
        time2.setEndTime("2016-08-08T00:00:01Z");

        TimeInterval time3 = new TimeInterval();
        time3.setEndTime("2016-08-08T00:00:02Z");

        Point p1 = new Point();
        p1.setInterval(time1);
        p1.setValue(new TypedValue());
        p1.getValue().setDoubleValue(77.0);

        Point p2 = new Point();
        p2.setInterval(time2);
        p2.setValue(new TypedValue());
        p2.getValue().setDoubleValue(88.0);

        Point p3 = new Point();
        p3.setInterval(time1);
        p3.setValue(new TypedValue());
        p3.getValue().setInt64Value(99l);

        Point p4 = new Point();
        p4.setInterval(time3);
        p4.setValue(new TypedValue());
        p4.getValue().setBoolValue(true);

        TimeSeries ts1 = new TimeSeries();
        ts1.setMetric(metric1);
        ts1.setResource(resource1);
        ts1.setPoints(Lists.newArrayList(p1, p2));
        ts1.setValueType("DOUBLE");
        ts1.setMetricKind("GAUGE");

        // This TimeSeries will be skipped because it has the wrong metric type.
        TimeSeries ts2 = new TimeSeries();
        ts2.setMetric(metric2);
        ts2.setResource(resource1);
        ts2.setPoints(Lists.newArrayList(p2));
        ts2.setValueType("DOUBLE");
        ts2.setMetricKind("GAUGE");

        TimeSeries ts3 = new TimeSeries();
        ts3.setMetric(metric3);
        ts3.setResource(resource1);
        ts3.setPoints(Lists.newArrayList(p3));
        ts3.setValueType("INT64");
        ts3.setMetricKind("GAUGE");

        TimeSeries ts4 = new TimeSeries();
        ts4.setMetric(metric4);
        ts4.setResource(resource2);
        ts4.setPoints(Lists.newArrayList(p4));
        ts4.setValueType("BOOL");
        ts4.setMetricKind("GAUGE");

        ListTimeSeriesResponse response = new ListTimeSeriesResponse();
        response.setTimeSeries(Lists.newArrayList(ts1, ts2, ts3, ts4));

        List<String> allMetrics = new ArrayList<>();
        allMetrics.add("custom.googleapis.com/test");

        Map<String, Map<String, Record>> byTime = new TreeMap<>();
        GoogleMonitoringIngester ingester = new GoogleMonitoringIngester(conf);
        ingester.processResponse(response, allMetrics, byTime);

        assertEquals(3, byTime.size());

        Map<String, Record> recordMap1 = byTime.get("2016-08-08T00:00:00Z");
        List<Record> records1 = Lists.newArrayList(recordMap1.values());
        assertEquals(2, records1.size());
        assertEquals(2, records1.get(0).attributes.size());
        assertEquals("foo", records1.get(0).attributes.get("attr1"));
        assertEquals("baz", records1.get(0).attributes.get("attr2"));
        assertEquals(99, records1.get(0).values.get("custom.googleapis.com/test"), 0.0);
        assertEquals(2, records1.get(1).attributes.size());
        assertEquals("foo", records1.get(1).attributes.get("attr1"));
        assertEquals("bar", records1.get(1).attributes.get("attr2"));
        assertEquals(77, records1.get(1).values.get("custom.googleapis.com/test"), 0.0);

        Map<String, Record> recordMap2 = byTime.get("2016-08-08T00:00:01Z");
        List<Record> records2 = Lists.newArrayList(recordMap2.values());
        assertEquals(1, records2.size());
        assertEquals(2, records2.get(0).attributes.size());
        assertEquals("foo", records2.get(0).attributes.get("attr1"));
        assertEquals("bar", records2.get(0).attributes.get("attr2"));
        assertEquals(88, records2.get(0).values.get("custom.googleapis.com/test"), 0.0);

        Map<String, Record> recordMap3 = byTime.get("2016-08-08T00:00:02Z");
        List<Record> records3 = Lists.newArrayList(recordMap3.values());
        assertEquals(1, records3.size());
        assertEquals(2, records3.get(0).attributes.size());
        assertEquals("foo", records3.get(0).attributes.get("attr1"));
        assertEquals("bar", records3.get(0).attributes.get("attr2"));
        assertEquals(1.0, records3.get(0).values.get("custom.googleapis.com/test"), 0.0);
    }

    @Test
    public void testConvertToStream() throws Exception {
        Map<String, Map<String, Record>> byTime = new TreeMap<>();

        Record record1 = new Record();
        record1.attributes = new TreeMap<>();
        record1.attributes.put("attr1", "foo");
        record1.attributes.put("attr2", "bar");
        record1.values = new HashMap<>();
        record1.values.put("custom.googleapis.com/test", 77.0);

        Record record2 = new Record();
        record2.attributes = new TreeMap<>();
        record2.attributes.put("attr1", "foo");
        record2.attributes.put("attr2", "baz");
        record2.values = new HashMap<>();
        record2.values.put("custom.googleapis.com/test", 88.0);

        // This record is incomplete and should be skipped.
        Record record3 = new Record();
        record3.attributes = new TreeMap<>();
        record3.attributes.put("attr1", "foo");
        record3.attributes.put("attr2", "bar");
        record3.values = new HashMap<>();

        Record record4 = new Record();
        record4.attributes = new TreeMap<>();
        record4.attributes.put("attr1", "foo");
        record4.attributes.put("attr2", "bar");
        record4.values = new HashMap<>();
        record4.values.put("custom.googleapis.com/test", 99.0);

        Map<String, Record> recordMap1 = new HashMap<>();
        recordMap1.put("attr-map-key1", record1);
        recordMap1.put("attr-map-key2", record2);

        Map<String, Record> recordMap2 = new HashMap<>();
        recordMap2.put("attr-map-key1", record3);

        Map<String, Record> recordMap3 = new HashMap<>();
        recordMap3.put("attr-map-key1", record4);

        byTime.put("2016-08-08T00:00:00Z", recordMap1);
        byTime.put("2016-08-08T00:00:01Z", recordMap2);
        byTime.put("2016-08-08T00:00:02Z", recordMap3);

        GoogleMonitoringIngester ingester = new GoogleMonitoringIngester(conf);
        MBStream<Datum> stream = ingester.convertToStream(byTime);

        List<Datum> data = stream.drain();
        assertEquals(3, data.size(), 0.0);

        assertEquals(77, data.get(0).metrics().getEntry(0), 0.0);
        assertEquals(0, data.get(0).attributes().get(0), 0.0);
        assertEquals(1, data.get(0).attributes().get(1), 0.0);

        assertEquals(88, data.get(1).metrics().getEntry(0), 0.0);
        assertEquals(0, data.get(1).attributes().get(0), 0.0);
        assertEquals(2, data.get(1).attributes().get(1), 0.0);

        assertEquals(99, data.get(2).metrics().getEntry(0), 0.0);
        assertEquals(0, data.get(2).attributes().get(0), 0.0);
        assertEquals(1, data.get(2).attributes().get(1), 0.0);
    }
}
