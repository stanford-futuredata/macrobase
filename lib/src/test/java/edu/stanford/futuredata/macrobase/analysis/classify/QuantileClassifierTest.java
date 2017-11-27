package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuantileClassifierTest {
    private int length = 100;
    private DataFrame df;
    private double[] quantiles;
    private List<String> quantileColumnNames;
    private LinkedHashMap<String, Double> quantileColumnsMap = new LinkedHashMap<String, Double>() {{
        put("q0.0", 0.0);
        put("q0.1", 0.1);
        put("q0.5", 0.5);
        put("q0.9", 0.9);
        put("q1.0", 1.0);
    }};
    private double[] rawData;
    private List<double[]> rawGroups;

    @Before
    public void setUp() {
        df = new DataFrame();
        double[] counts = new double[length];
        double[] means = new double[length];
        int c = 0;
        quantileColumnNames = new ArrayList<String>();
        quantiles = new double[quantileColumnsMap.size()];
        for (Map.Entry<String, Double> entry : quantileColumnsMap.entrySet()) {
            quantileColumnNames.add(entry.getKey());
            quantiles[c++] = entry.getValue();
        }

        rawData = new double[length*101];
        rawGroups = new ArrayList<>();
        int d = 0;
        List<double[]> quantileColumns = new ArrayList<double[]>();
        for (int i = 0; i < quantiles.length; i++) {
            quantileColumns.add(new double[length]);
        }
        for (int i = 0; i < length; i++) {
            double[] rawGroup = new double[101];
            int g = 0;
            for (int j = i-50; j <= i+50; j++) {
                rawData[d++] = j;
                rawGroup[g++] = j;
            }
            rawGroups.add(rawGroup);
            counts[i] = 101;
            means[i] = i;
            for (int j = 0; j < quantiles.length; j++) {
                quantileColumns.get(j)[i] = i + (quantiles[j] - 0.5) * 100;
            }
        }
        df.addDoubleColumn("count", counts);
        df.addDoubleColumn("mean", means);
        for (int i = 0; i < quantiles.length; i++) {
            df.addDoubleColumn(quantileColumnNames.get(i), quantileColumns.get(i));
        }
    }

    @Test
    public void testClassify() throws Exception {
        assertEquals(length, df.getNumRows());
        QuantileClassifier ac = new QuantileClassifier(
                "count",
                quantileColumnsMap
        );
        ac.process(df);
        DataFrame output = ac.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(7, df.getSchema().getNumColumns());
        assertEquals(8, output.getSchema().getNumColumns());

        Percentile percentile = new Percentile();
        percentile.setData(rawData);
        double trueLowCutoff = percentile.evaluate(1);
        double trueHighCutoff = percentile.evaluate(99);
        assertEquals(trueLowCutoff, ac.getLowCutoff(), 5.0);
        assertEquals(trueHighCutoff, ac.getHighCutoff(), 5.0);

        double[] outliers = output.getDoubleColumnByName("_OUTLIER");

        for (int i = 0; i < outliers.length; i++) {
            int trueNumOutliers = 0;
            double[] rawGroup = rawGroups.get(i);
            for (int j = 0; j < rawGroup.length; j++) {
                if (rawGroup[j] < trueLowCutoff || rawGroup[j] > trueHighCutoff) {
                    trueNumOutliers++;
                }
            }
            assertEquals(trueNumOutliers, outliers[i], 5.0);
        }
    }

    @Test
    public void testConfigure() throws Exception {
        QuantileClassifier ac = new QuantileClassifier(
                "col1",
                new LinkedHashMap<>()
        );
        ac.setCountColumnName("count");
        ac.setQuantileColumnNames(quantileColumnNames);
        ac.setQuantiles(quantiles);
        ac.setIncludeHigh(false);
        ac.setIncludeLow(true);
        ac.setOutputColumnName("_OUT");
        ac.setPercentile(5.0);

        ac.process(df);
        DataFrame output = ac.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());

        Percentile percentile = new Percentile();
        percentile.setData(rawData);
        double trueLowCutoff = percentile.evaluate(5);
        assertEquals(trueLowCutoff, ac.getLowCutoff(), 5.0);

        double[] outliers = output.getDoubleColumnByName("_OUT");

        for (int i = 0; i < outliers.length; i++) {
            int trueNumOutliers = 0;
            double[] rawGroup = rawGroups.get(i);
            for (int j = 0; j < rawGroup.length; j++) {
                if (rawGroup[j] < trueLowCutoff) {
                    trueNumOutliers++;
                }
            }
            assertEquals(trueNumOutliers, outliers[i], 5.0);
        }
    }
}