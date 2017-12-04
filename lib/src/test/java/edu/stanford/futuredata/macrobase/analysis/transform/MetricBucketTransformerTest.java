package edu.stanford.futuredata.macrobase.analysis.transform;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class MetricBucketTransformerTest {
    @Test
    public void testSingleMetric() throws Exception {
        DataFrame df = new DataFrame();
        int n = 100;
        double[] values = new double[n];
        for (int i = 0; i < n; i++) {
            values[i] = i;
        }
        df.addColumn("values", values);

        MetricBucketTransformer t = new MetricBucketTransformer("values");
        t.process(df);

        DataFrame tdf = t.getResults();
        assertEquals(2, tdf.getSchema().getNumColumns());
        assertEquals(n, tdf.getNumRows());
        String[] newCol = tdf.getStringColumnByName(t.getTransformedColumnNames().get(0));
        Set<String> distinct = new HashSet<>();
        Collections.addAll(distinct, newCol);
        assertEquals(3, distinct.size());

        t.setSimpleBucketValues(true);
        t.process(df);
        tdf = t.getResults();
        assertEquals(2, tdf.getSchema().getNumColumns());
        assertEquals(n, tdf.getNumRows());
        newCol = tdf.getStringColumnByName(t.getTransformedColumnNames().get(0));
        distinct = new HashSet<>();
        Collections.addAll(distinct, newCol);
        assertEquals(3, distinct.size());
    }

    @Test
    public void testComplexUsage() throws Exception {
        DataFrame df = new DataFrame();
        int n = 100;
        int d = 3;
        List<String> metricColumns = new ArrayList<>(d);
        for (int j = 0; j < d; j++) {
            double[] values = new double[n];
            for (int i = 0; i < n; i++) {
                values[i] = i;
            }
            String curColumnName = "m"+j;
            metricColumns.add(curColumnName);
            df.addColumn(curColumnName, values);
        }

        MetricBucketTransformer t = new MetricBucketTransformer(metricColumns);
        double[] boundaries = {20.0,50.0,80.0};
        t.setBoundaryPercentiles(boundaries);
        t.process(df);
        DataFrame tdf = t.getResults();

        assertEquals(2*d, tdf.getSchema().getNumColumns());
        assertEquals(n, tdf.getNumRows());
        String[] newCol = tdf.getStringColumnByName(t.getTransformedColumnNames().get(0));
        Set<String> distinct = new HashSet<>();
        Collections.addAll(distinct, newCol);
        assertEquals(4, distinct.size());
    }
}