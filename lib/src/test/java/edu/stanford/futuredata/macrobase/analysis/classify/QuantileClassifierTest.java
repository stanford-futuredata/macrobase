package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuantileClassifierTest {
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

    @Before
    public void setUp() {
        df = new DataFrame();
        int length = 1000;
        double[] counts = new double[length];
        double[] means = new double[length];
        int c = 0;
        quantileColumnNames = new ArrayList<String>();
        quantiles = new double[quantileColumnsMap.size()];
        for (Map.Entry<String, Double> entry : quantileColumnsMap.entrySet()) {
            quantileColumnNames.add(entry.getKey());
            quantiles[c++] = entry.getValue();
        }

        List<double[]> quantileColumns = new ArrayList<double[]>();
        for (int i = 0; i < quantiles.length; i++) {
            quantileColumns.add(new double[length]);
        }
        for (int i = 0; i < length; i++) {
            counts[i] = i;
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
        assertEquals(1000, df.getNumRows());
        QuantileClassifier ac = new QuantileClassifier("count", "mean", quantileColumnsMap);
        ac.process(df);
        DataFrame output = ac.getResults();
        assertEquals(100.0, ac.getLowCutoff(), 0.01);
        assertEquals(994.0, ac.getHighCutoff(), 0.01);
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(7, df.getSchema().getNumColumns());
        assertEquals(8, output.getSchema().getNumColumns());

        int numAtLeastOneOutlier = 0;
        double totalOutliers = 0.0;
        double[] outliers = output.getDoubleColumnByName("_OUTLIER");
        for (double numOutliers : outliers) {
            totalOutliers += numOutliers;
            if (numOutliers >= 1.0) numAtLeastOneOutlier++;
        }
        assertTrue(totalOutliers >= 20520 && totalOutliers <= 20530);
        assertEquals(204, numAtLeastOneOutlier);

        DataFrame groupsWithAtLeastOneOutlier = output.filter(
                "_OUTLIER", (double d) -> d >= 1.0
        );
        double[] means = groupsWithAtLeastOneOutlier.getDoubleColumnByName("mean");
        for (double mean : means) {
            assertTrue(mean <= 149.0 || mean >= 945.0);
        }
    }

    @Test
    public void testConfigure() throws Exception {
        QuantileClassifier ac = new QuantileClassifier("col1", "col2",
                new LinkedHashMap<>());
        ac.setMeanColumnName("mean");
        ac.setCountColumnName("count");
        ac.setQuantileColumnNames(quantileColumnNames);
        ac.setQuantiles(quantiles);
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
        assertTrue(totalOutliers >= 25280 && totalOutliers <= 25290);

        DataFrame groupsWithAtLeastOneOutlier = output.filter(
                "_OUT", (double d) -> d >= 1.0
        );
        double[] means = groupsWithAtLeastOneOutlier.getDoubleColumnByName("mean");
        for (double mean : means) {
            assertTrue(mean <= 272);
        }
    }
}