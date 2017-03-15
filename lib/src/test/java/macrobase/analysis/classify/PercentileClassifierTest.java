package macrobase.analysis.classify;

import macrobase.datamodel.DataFrame;
import org.junit.Before;
import org.junit.Test;

import java.util.function.DoublePredicate;

import static org.junit.Assert.*;

public class PercentileClassifierTest {
    private DataFrame df;

    @Before
    public void setUp() {
        df = new DataFrame();
        double[] vals = new double[1000];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = i;
        }
        df.addDoubleColumn("val", vals);
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
}