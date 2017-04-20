package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiMADClassifierTest {
    private DataFrame df;

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
            a2[i] = (i+10)%100;
        }
        df.addDoubleColumn("a2", a2);
    }

    @Test
    public void testClassify() throws Exception {
        assertEquals(100, df.getNumRows());
        MultiMADClassifier mad = new MultiMADClassifier("a1", "a2").setPercentile(10);
        mad.process(df);
        DataFrame output = mad.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(2, df.getSchema().getNumColumns());
        assertEquals(3, output.getSchema().getNumColumns());

        DataFrame outliers = output.filter(
                mad.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == 8);
        double[] a1 = outliers.getDoubleColumnByName("a1");
        for (double val : a1) {
            assertTrue(val <= 1 || val >= 98 || (val >= 88 && val <= 91));
        }
    }
}