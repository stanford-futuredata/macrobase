package edu.stanford.futuredata.macrobase.analysis.summary;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.*;

public class MovingAverageTest {
    private DataFrame testDF;

    public static DataFrame getTestDF() {
        // Create a block of ones surrounded by a block of zeroes
        int n = 100;
        int blockStart = 10;
        int blockEnd = 20;
        double[] times = new double[n];
        for (int i = 0; i < n; i++) {
            times[i] = (double)i;
        }
        double[] vals = new double[n];
        for (int i = blockStart; i < blockEnd; i++) {
            vals[i] = 1.0;
        }
        DataFrame data = new DataFrame();
        data.addColumn("time", times);
        data.addColumn("val", vals);

        return data;
    }

    @Before
    public void setUp() {
        testDF = getTestDF();
    }

    @Test
    public void testPanes() throws Exception {
        MovingAverage m = new MovingAverage("val", 3);
        int paneSize = 10;
        int n = testDF.getNumRows();
        int numPanes = n / paneSize;
        for (int i = 0; i < numPanes; i++) {
            double startTime = i * paneSize;
            double endtime = (i+1) * paneSize;
            DataFrame curPane = testDF.filter("time", (double t) -> (t >= startTime && t < endtime));
            m.process(curPane);
            if (i == 0) {
                assertEquals(0.0, m.getResults(), 0.0);
            } else if (i == 1) {
                assertEquals(0.5, m.getResults(), 1e-10);
            } else if (i == 2) {
                assertEquals(1.0/3, m.getResults(), 1e-5);
            } else if (i > 3) {
                assertEquals(0.0, m.getResults(), 0.0);
            }
        }
    }
}