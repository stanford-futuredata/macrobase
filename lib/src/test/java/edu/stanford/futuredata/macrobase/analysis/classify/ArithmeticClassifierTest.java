package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ArithmeticClassifierTest {
    private DataFrame df;

    @Before
    public void setUp() {
        df = new DataFrame();
        int length = 1000;
        double[] counts = new double[length];
        double[] means = new double[length];
        double[] stds = new double[length];
        for (int i = 0; i < length; i++) {
            counts[i] = i;
            means[i] = i;
            stds[i] = 10;
        }
        df.addColumn("count", counts);
        df.addColumn("mean", means);
        df.addColumn("std", stds);
    }

    @Test
    public void testClassify() throws Exception {
        assertEquals(1000, df.getNumRows());
        ArithmeticClassifier ac = new ArithmeticClassifier("count", "mean", "std");
        ac.process(df);
        DataFrame output = ac.getResults();
        assertEquals(100.0, ac.getLowCutoff(), 0.01);
        assertEquals(994.0, ac.getHighCutoff(), 0.01);
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(3, df.getSchema().getNumColumns());
        assertEquals(4, output.getSchema().getNumColumns());

        int numAtLeastOneOutlier = 0;
        double totalOutliers = 0.0;
        double[] outliers = output.getDoubleColumnByName("_OUTLIER");
        for (double numOutliers : outliers) {
            totalOutliers += numOutliers;
            if (numOutliers >= 1.0) numAtLeastOneOutlier++;
        }
        assertTrue(totalOutliers >= 12300 && totalOutliers <= 12350);
        assertEquals(160, numAtLeastOneOutlier);

        DataFrame groupsWithAtLeastOneOutlier = output.filter(
                "_OUTLIER", (double d) -> d >= 1.0
        );
        double[] means = groupsWithAtLeastOneOutlier.getDoubleColumnByName("mean");
        for (double mean : means) {
            assertTrue(mean <= 124.0 || mean >= 964.0);
        }
    }

    @Test
    public void testConfigure() throws Exception {
        ArithmeticClassifier ac = new ArithmeticClassifier("col1", "col2", "col3");
        ac.setMeanColumnName("mean");
        ac.setCountColumnName("count");
        ac.setStdColumnName("std");
        ac.setIncludeHigh(false);
        ac.setIncludeLow(true);
        ac.setOutputColumnName("_OUT");
        ac.setPercentile(5.0);

        ac.process(df);
        DataFrame output = ac.getResults();
        double lowCutoff = ac.getLowCutoff();
        assertTrue(lowCutoff == 223.0);
        assertEquals(df.getNumRows(), output.getNumRows());

        int numAtLeastOneOutlier = 0;
        double totalOutliers = 0.0;
        double[] outliers = output.getDoubleColumnByName("_OUT");
        for (double numOutliers : outliers) {
            totalOutliers += numOutliers;
            if (numOutliers >= 1.0) numAtLeastOneOutlier++;
        }
        assertTrue(totalOutliers >= 24900 && totalOutliers <= 25000);

        DataFrame groupsWithAtLeastOneOutlier = output.filter(
                "_OUT", (double d) -> d >= 1.0
        );
        double[] means = groupsWithAtLeastOneOutlier.getDoubleColumnByName("mean");
        for (double mean : means) {
            assertTrue(mean <= 249);
        }
    }
}