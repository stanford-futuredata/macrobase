package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BoundsClassifierTest {
    private int length = 100;
    private DataFrame df;
    private double[] rawData;
    private List<double[]> rawGroups;

    @Before
    public void setUp() {
        df = new DataFrame();
        double[] counts = new double[length];
        double[] means = new double[length];
        double[] stds = new double[length];
        double[] mins = new double[length];
        double[] maxs = new double[length];

        rawData = new double[length*101];
        rawGroups = new ArrayList<>();
        int d = 0;
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
            mins[i] = i-50;
            maxs[i] = i+50;
            double std = 0.0;
            for (double raw : rawGroup) {
                std += (raw - i) * (raw - i);
            }
            stds[i] = Math.sqrt(std / (rawGroup.length - 1));
        }
        df.addDoubleColumn("count", counts);
        df.addDoubleColumn("mean", means);
        df.addDoubleColumn("std", stds);
        df.addDoubleColumn("min", mins);
        df.addDoubleColumn("max", maxs);
    }

    @Test
    public void testClassify() throws Exception {
        assertEquals(length, df.getNumRows());
        BoundsClassifier bc = new BoundsClassifier("count", "mean", "std", "min", "max");
        bc.process(df);
        DataFrame output = bc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(5, df.getSchema().getNumColumns());
        assertEquals(6, output.getSchema().getNumColumns());

        Percentile percentile = new Percentile();
        percentile.setData(rawData);
        double trueLowCutoff = percentile.evaluate(1);
        double trueHighCutoff = percentile.evaluate(99);
        assertEquals(trueLowCutoff, bc.getLowCutoff(), 40.0);
        assertEquals(trueHighCutoff, bc.getHighCutoff(), 40.0);

        double[] outliers = output.getDoubleColumnByName("_OUTLIER");

        for (int i = 0; i < outliers.length; i++) {
            int trueNumOutliers = 0;
            double[] rawGroup = rawGroups.get(i);
            for (int j = 0; j < rawGroup.length; j++) {
                if (rawGroup[j] < trueLowCutoff || rawGroup[j] > trueHighCutoff) {
                    trueNumOutliers++;
                }
            }
            assertEquals(trueNumOutliers, outliers[i], 10.0);
        }
    }

    @Test
    public void testConfigure() throws Exception {
        BoundsClassifier bc = new BoundsClassifier("col1", "col2", "col3", "col4", "col5");
        bc.setMeanColumnName("mean");
        bc.setCountColumnName("count");
        bc.setStdColumnName("std");
        bc.setMinColumnName("min");
        bc.setMaxColumnName("max");
        bc.setIncludeHigh(false);
        bc.setIncludeLow(true);
        bc.setOutputColumnName("_OUT");
        bc.setPercentile(5.0);

        bc.process(df);
        DataFrame output = bc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());

        Percentile percentile = new Percentile();
        percentile.setData(rawData);
        double trueLowCutoff = percentile.evaluate(5);
        assertEquals(trueLowCutoff, bc.getLowCutoff(), 25.0);

        double[] outliers = output.getDoubleColumnByName("_OUT");

        for (int i = 0; i < outliers.length; i++) {
            int trueNumOutliers = 0;
            double[] rawGroup = rawGroups.get(i);
            for (int j = 0; j < rawGroup.length; j++) {
                if (rawGroup[j] < trueLowCutoff) {
                    trueNumOutliers++;
                }
            }
            assertEquals(trueNumOutliers, outliers[i], 15.0);
        }
    }
}