package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiMADClassifierTest {
    private DataFrame df;
    private DataFrame output;
    private DataFrame outliers;
    private int numOutliers;
    private MultiMADClassifier mad;

    @Before
    public void setUp() {
        df = new DataFrame();
        double[] a1 = new double[100];
        for (int i = 0; i < a1.length; i++) {
            a1[i] = i;
        }
        df.addDoubleColumn("a1", a1);
        double[] a2 = new double[100];
        for (int i = 0; i < a2.length; i++) {
            a2[i] = 2*i;
        }
        df.addDoubleColumn("a2", a2);
    }

    @Test
    public void testClassify() throws Exception {
        assertEquals(100, df.getNumRows());
        mad = new MultiMADClassifier("a1", "a2").setPercentile(10);

        mad.process(df);
        output = mad.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(2, df.getSchema().getNumColumns());
        assertEquals(4, output.getSchema().getNumColumns());

        outliers = output.filter(
                "a1" + mad.getOutputColumnSuffix(), (double d) -> d != 0.0
        );
        numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == 4);
        double[] a1 = outliers.getDoubleColumnByName("a1");
        for (double val : a1) {
            assertTrue(val <= 1 || val >= 98);
        }

        outliers = output.filter(
                "a2" + mad.getOutputColumnSuffix(), (double d) -> d != 0.0
        );
        numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == 4);
        double[] a2 = outliers.getDoubleColumnByName("a2");
        for (double val : a2) {
            assertTrue(val <= 2 || val >= 196);
        }
    }

    @Test
    public void testConfigure() throws Exception {
        mad = new MultiMADClassifier("notcolumn");
        mad = new MultiMADClassifier(Arrays.asList("notcolumn1", "notcolumn2"));
        mad.setColumnNames("a1", "a2")
                .setColumnNames(Arrays.asList("a1", "a2"))
                .setIncludeHigh(false)
                .setIncludeLow(true)
                .setOutputColumnSuffix("_OUT")
                .setPercentile(10)
                .setZscore(1.2)
                .setSamplingRate(0.5);

        mad.process(df);
        output = mad.getResults();
        boolean isIncludeHigh = mad.isIncludeHigh();
        assertTrue(isIncludeHigh == false);
        assertEquals(df.getNumRows(), output.getNumRows());

        outliers = output.filter(
                "a1_OUT", (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers <= 100);
    }

    @Test
    public void testSampling() throws Exception {
        assertEquals(100, df.getNumRows());
        mad = new MultiMADClassifier("a1", "a2").setSamplingRate(0.5).setUseReservoirSampling(true);
        mad.process(df);
        output = mad.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(2, df.getSchema().getNumColumns());
        assertEquals(4, output.getSchema().getNumColumns());
        assertEquals(mad.getSamplingRate(), 0.5, 0.0);

        mad = new MultiMADClassifier("a1", "a2").setSamplingRate(0.1);
        mad.process(df);
        output = mad.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(2, df.getSchema().getNumColumns());
        assertEquals(4, output.getSchema().getNumColumns());
        assertEquals(mad.getSamplingRate(), 0.1, 0.0);
    }

    @Test
    public void testLargeInput() throws Exception {
        df = new DataFrame();
        double[] a1 = new double[200000];
        for (int i = 0; i < a1.length; i++) {
            a1[i] = i;
        }
        df.addDoubleColumn("a1", a1);

        assertEquals(200000, df.getNumRows());
        mad = new MultiMADClassifier("a1").setPercentile(10).setUseParallel(true);
        mad.process(df);
        output = mad.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(1, df.getSchema().getNumColumns());
        assertEquals(2, output.getSchema().getNumColumns());

        outliers = output.filter(
                "a1" + mad.getOutputColumnSuffix(), (double d) -> d != 0.0
        );
        numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == 9998);
        a1 = outliers.getDoubleColumnByName("a1");
        for (double val : a1) {
            assertTrue(val < 4999 || val > 195000);
        }
    }
}