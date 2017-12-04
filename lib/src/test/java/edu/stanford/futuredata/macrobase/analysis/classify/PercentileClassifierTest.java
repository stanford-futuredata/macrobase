package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PercentileClassifierTest {
    private DataFrame df;

    @Before
    public void setUp() {
        df = new DataFrame();
        double[] vals = new double[1000];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = i;
        }
        df.addColumn("val", vals);
    }

    @Test
    public void testClassify() throws Exception {
        assertEquals(1000, df.getNumRows());
        PercentileClassifier pc = new PercentileClassifier("val");
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(1, df.getSchema().getNumColumns());
        assertEquals(2, output.getSchema().getNumColumns());

        DataFrame outliers = output.filter(
                pc.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers >= 8 && numOutliers <= 12);
        double[] vals = outliers.getDoubleColumnByName("val");
        for (double val : vals) {
            assertTrue(val < 10 || val > 990);
        }
    }

    @Test
    public void testConfigure() throws Exception {
        PercentileClassifier pc = new PercentileClassifier("notcolumn");
        pc.setColumnName("val");
        pc.setIncludeHigh(false);
        pc.setIncludeLow(true);
        pc.setOutputColumnName("_OUT");
        pc.setPercentile(10);

        pc.process(df);
        DataFrame output = pc.getResults();
        double lowCutoff = pc.getLowCutoff();
        assertTrue(lowCutoff > 90.0 && lowCutoff < 110.0);
        assertEquals(df.getNumRows(), output.getNumRows());

        DataFrame outliers = output.filter(
                "_OUT", (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers >= 90 && numOutliers <= 110);
    }
}