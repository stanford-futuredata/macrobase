package edu.stanford.futuredata.macrobase.analysis.classify;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/**
 * Created by fabuzaid21 on 8/4/17.
 */
public class PredicateClassifierTest {

    private static final int NUM_ROWS = 1000;
    private static final int METRIC_CARDINALITY = 4;

    private DataFrame df;

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        df = new DataFrame();
        final int num_rows_per_value = NUM_ROWS / METRIC_CARDINALITY;
        double[] vals = new double[NUM_ROWS];
        double metricVal = 0.0;
        int j = 0;
        for (int i = 0; i < METRIC_CARDINALITY; ++i) {
            for (; j < num_rows_per_value * (i + 1); ++j) {
                vals[j] = metricVal;
            }
            metricVal += 1.0;
        }
        df.addDoubleColumn("val", vals);
    }

    @Test
    public void testEquals() throws Exception {
        assertEquals(NUM_ROWS, df.getNumRows());
        PredicateClassifier pc = new PredicateClassifier("val", "==", 0.0);
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(1, df.getSchema().getNumColumns());
        assertEquals(2, output.getSchema().getNumColumns());

        DataFrame outliers = output.filter(
                pc.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == NUM_ROWS / METRIC_CARDINALITY);
        double[] vals = outliers.getDoubleColumnByName("val");
        for (double val : vals) {
            assertTrue(val == 0.0);
        }
    }

    @Test
    public void testNotEquals() throws Exception {
        assertEquals(NUM_ROWS, df.getNumRows());
        PredicateClassifier pc = new PredicateClassifier("val", "!=", 0.0);
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(1, df.getSchema().getNumColumns());
        assertEquals(2, output.getSchema().getNumColumns());

        DataFrame outliers = output.filter(
                pc.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == (METRIC_CARDINALITY - 1) * NUM_ROWS / METRIC_CARDINALITY);
        double[] vals = outliers.getDoubleColumnByName("val");
        for (double val : vals) {
            assertTrue(val != 0.0);
        }
    }

    @Test
    public void testLessThan() throws Exception {
        assertEquals(NUM_ROWS, df.getNumRows());
        PredicateClassifier pc = new PredicateClassifier("val", "<", 3.0);
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(1, df.getSchema().getNumColumns());
        assertEquals(2, output.getSchema().getNumColumns());

        DataFrame outliers = output.filter(
                pc.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == (METRIC_CARDINALITY - 1) * NUM_ROWS / METRIC_CARDINALITY);
        double[] vals = outliers.getDoubleColumnByName("val");
        for (double val : vals) {
            assertTrue(val < 3.0);
        }
    }

    @Test
    public void testGreaterThan() throws Exception {
        assertEquals(NUM_ROWS, df.getNumRows());
        PredicateClassifier pc = new PredicateClassifier("val", ">", 1.0);
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(1, df.getSchema().getNumColumns());
        assertEquals(2, output.getSchema().getNumColumns());

        DataFrame outliers = output.filter(
                pc.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == NUM_ROWS / 2);
        double[] vals = outliers.getDoubleColumnByName("val");
        for (double val : vals) {
            assertTrue(val > 1.0);
        }
    }


    @Test
    public void testLessThanOrEqual() throws Exception {
        assertEquals(NUM_ROWS, df.getNumRows());
        PredicateClassifier pc = new PredicateClassifier("val", "<=", 1.0);
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(1, df.getSchema().getNumColumns());
        assertEquals(2, output.getSchema().getNumColumns());

        DataFrame outliers = output.filter(
                pc.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == NUM_ROWS / 2);
        double[] vals = outliers.getDoubleColumnByName("val");
        for (double val : vals) {
            assertTrue(val <= 1.0);
        }
    }


    @Test
    public void testGreaterThanOrEqual() throws Exception {
        assertEquals(NUM_ROWS, df.getNumRows());
        PredicateClassifier pc = new PredicateClassifier("val", ">=", 3.0);
        pc.process(df);
        DataFrame output = pc.getResults();
        assertEquals(df.getNumRows(), output.getNumRows());
        assertEquals(1, df.getSchema().getNumColumns());
        assertEquals(2, output.getSchema().getNumColumns());

        DataFrame outliers = output.filter(
                pc.getOutputColumnName(), (double d) -> d != 0.0
        );
        int numOutliers = outliers.getNumRows();
        assertTrue(numOutliers == NUM_ROWS / METRIC_CARDINALITY);
        double[] vals = outliers.getDoubleColumnByName("val");
        for (double val : vals) {
            assertTrue(val >= 3.0);
        }
    }
}